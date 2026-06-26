#!/usr/bin/env python3
"""Watch Lidarr command queue; grab via slskd when user searches an album."""
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lidarr-slskd")

LIDARR_URL  = os.environ.get("LIDARR_URL",    "http://lidarr:8686")
LIDARR_KEY  = os.environ.get("LIDARR_API_KEY", "")
SLSKD_URL   = os.environ.get("SLSKD_URL",     "http://slskd:5030")
SLSKD_KEY   = os.environ.get("SLSKD_API_KEY",  "")
DB_PATH     = os.environ.get("DB_PATH",        "/data/grabs.db")
POLL_SECS      = int(os.environ.get("POLL_SECS",      "10"))
SEARCH_WAIT    = int(os.environ.get("SEARCH_WAIT",    "45"))
DOWNLOAD_TIMEOUT = int(os.environ.get("DOWNLOAD_TIMEOUT", "1800"))  # 30 min
SWEEP_SECS       = int(os.environ.get("SWEEP_SECS",       "120"))   # check for manual downloads every 2 min
NTFY_URL        = os.environ.get("NTFY_URL", "")
DOWNLOADS_PATH  = os.environ.get("DOWNLOADS_PATH", "/downloads")
MUSIC_PATH      = os.environ.get("MUSIC_PATH", "/music")
NAVIDROME_URL   = os.environ.get("NAVIDROME_URL", "http://navidrome:4533")
NAVIDROME_USER  = os.environ.get("NAVIDROME_USER", "")
NAVIDROME_PASS  = os.environ.get("NAVIDROME_PASS", "")
DISCOGS_TOKEN   = os.environ.get("DISCOGS_TOKEN", "")
NAVIDROME_DB_PATH = os.environ.get("NAVIDROME_DB_PATH", "")
BEETS_CONFIG    = "/beets/config.yaml"
MIN_MP3_BITRATE = 256   # kbps — reject sources below this


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def req(url, method="GET", data=None, headers=None):
    body = json.dumps(data).encode() if data is not None else None
    r = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    resp = urllib.request.urlopen(r, timeout=30)
    raw = resp.read()
    return json.loads(raw) if raw else {}


def lidarr(path, method="GET", data=None):
    return req(f"{LIDARR_URL}/api/v1{path}", method, data,
               {"X-Api-Key": LIDARR_KEY, "Content-Type": "application/json"})


def slskd(path, method="GET", data=None):
    return req(f"{SLSKD_URL}/api/v0{path}", method, data,
               {"X-API-Key": SLSKD_KEY, "Content-Type": "application/json"})


def notify(title, message):
    if not NTFY_URL:
        return
    try:
        urllib.request.urlopen(
            urllib.request.Request(
                NTFY_URL, data=message.encode(), method="POST",
                headers={"Title": title, "Priority": "default", "Tags": "musical_note"}
            ), timeout=10
        )
    except Exception:
        pass


# ── Database ──────────────────────────────────────────────────────────────────

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    c = db()
    c.execute("""CREATE TABLE IF NOT EXISTS grabs (
        lidarr_id        INTEGER PRIMARY KEY,
        artist           TEXT,
        album            TEXT,
        status           TEXT,
        source           TEXT,
        files            TEXT,
        dl_started       REAL,
        known_folders    TEXT,
        updated          TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS state (
        key   TEXT PRIMARY KEY,
        value TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS grab_failures (
        lidarr_id  INTEGER,
        source     TEXT,
        PRIMARY KEY (lidarr_id, source)
    )""")
    # Migration: add known_folders if upgrading from older schema
    cols = [r[1] for r in c.execute("PRAGMA table_info(grabs)").fetchall()]
    if "known_folders" not in cols:
        c.execute("ALTER TABLE grabs ADD COLUMN known_folders TEXT")
    c.commit()
    c.close()


def get_state(key, default=None):
    c = db()
    row = c.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    c.close()
    return row["value"] if row else default


def set_state(key, value):
    c = db()
    c.execute("INSERT INTO state (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
              (key, str(value)))
    c.commit()
    c.close()


def get_grab_status(lidarr_id):
    c = db()
    row = c.execute("SELECT status FROM grabs WHERE lidarr_id=?", (lidarr_id,)).fetchone()
    c.close()
    return row["status"] if row else None


def set_grab(lidarr_id, artist, album, status, source=None, files=None, dl_started=None, known_folders=None):
    c = db()
    c.execute("""INSERT INTO grabs (lidarr_id,artist,album,status,source,files,dl_started,known_folders,updated)
                 VALUES (?,?,?,?,?,?,?,?,datetime('now'))
                 ON CONFLICT(lidarr_id) DO UPDATE SET
                   status=excluded.status, source=excluded.source,
                   files=excluded.files,
                   dl_started=COALESCE(excluded.dl_started, dl_started),
                   known_folders=COALESCE(excluded.known_folders, known_folders),
                   updated=excluded.updated""",
              (lidarr_id, artist, album, status, source,
               json.dumps(files) if files else None, dl_started,
               json.dumps(known_folders) if known_folders is not None else None))
    c.commit()
    c.close()


def get_failed_sources(lidarr_id):
    """Return set of source usernames that have already failed for this album."""
    c = db()
    rows = c.execute("SELECT source FROM grab_failures WHERE lidarr_id=?", (lidarr_id,)).fetchall()
    c.close()
    return {r[0] for r in rows}


def record_failure(lidarr_id, source):
    c = db()
    c.execute("INSERT OR IGNORE INTO grab_failures (lidarr_id, source) VALUES (?,?)", (lidarr_id, source))
    c.commit()
    c.close()


# ── slskd search + grab ───────────────────────────────────────────────────────

def bitrate(attrs):
    for a in attrs:
        if a.get("type") == 1:
            return a.get("value", 0)
    return 0


def get_expected_track_count(lidarr_id):
    """Ask Lidarr how many tracks the album should have."""
    try:
        alb = lidarr(f"/album/{lidarr_id}")
        return alb.get("statistics", {}).get("totalTrackCount", 0)
    except Exception:
        return 0


def search_and_grab(lidarr_id, artist, album):
    query = f"{artist} {album}"
    log.info("Searching slskd: %s", query)

    # Bail if a matching folder already exists in the downloads directory
    try:
        al_lower = album.lower()
        ar_lower = artist.lower()
        audio_exts = {".mp3", ".m4a", ".flac", ".ogg"}
        for entry in os.scandir(DOWNLOADS_PATH):
            if not entry.is_dir():
                continue
            name_lower = entry.name.lower()
            if al_lower in name_lower or ar_lower in name_lower:
                audio = [f for f in os.listdir(entry.path)
                         if os.path.splitext(f)[1].lower() in audio_exts]
                if audio:
                    log.info("search_and_grab: %s – %s already in downloads (%s) — skipping",
                             artist, album, entry.name)
                    existing = [e.name for e in os.scandir(DOWNLOADS_PATH) if e.is_dir()]
                    set_grab(lidarr_id, artist, album, "downloading", known_folders=existing)
                    return
    except Exception as e:
        log.debug("Downloads folder pre-check failed: %s", e)

    set_grab(lidarr_id, artist, album, "searching")

    expected = get_expected_track_count(lidarr_id)
    min_tracks = expected if expected else 2
    log.info("Expecting %d tracks, requiring all %d", expected, min_tracks)

    try:
        result = slskd("/searches", "POST", {"searchText": query})
        sid = result.get("id")
    except Exception as e:
        log.warning("Search failed for %r: %s", query, e)
        set_grab(lidarr_id, artist, album, "failed")
        return

    deadline = time.time() + SEARCH_WAIT
    while time.time() < deadline:
        try:
            if slskd(f"/searches/{sid}").get("state") == "Completed":
                break
        except Exception:
            pass
        time.sleep(3)

    try:
        responses = slskd(f"/searches/{sid}/responses")
    except Exception:
        responses = []

    if not responses:
        log.warning("No results for %r", query)
        set_grab(lidarr_id, artist, album, "failed")
        return

    al = album.lower()
    ar = artist.lower()
    failed_sources = get_failed_sources(lidarr_id)
    if failed_sources:
        log.info("Excluding previously failed sources: %s", failed_sources)
    lossy_candidates = []
    flac_candidates  = []
    for resp in responses:
        username = resp.get("username", "")
        if username in failed_sources:
            continue
        speed = resp.get("uploadSpeed", 0)
        qlen  = resp.get("queueLength", 9999)
        files = resp.get("files", [])
        # MP3 / M4A pass
        lossy = [
            f for f in files
            if f.get("filename", "").lower().endswith((".mp3", ".m4a"))
            and (al in f.get("filename", "").lower() or ar in f.get("filename", "").lower())
        ]
        if len(lossy) >= min_tracks:
            brs   = [bitrate(f.get("attributes", [])) for f in lossy]
            avg   = sum(brs) / len(brs) if brs else 0
            is320 = avg >= 310 or any(b == 320 for b in brs)
            lossy_candidates.append((is320, avg, speed, -qlen, username, lossy))
            continue
        # FLAC fallback pass
        flacs = [
            f for f in files
            if f.get("filename", "").lower().endswith(".flac")
            and (al in f.get("filename", "").lower() or ar in f.get("filename", "").lower())
        ]
        if len(flacs) >= min_tracks:
            total_size = sum(f.get("size", 0) for f in flacs)
            flac_candidates.append((total_size, -speed, qlen, username, flacs))

    if lossy_candidates:
        lossy_candidates.sort(key=lambda c: (c[0], c[1], c[2], c[3]), reverse=True)
        candidates = [(c[4], c[5], "320kbps" if c[0] else f"{int(c[1])}kbps") for c in lossy_candidates]
    elif flac_candidates:
        flac_candidates.sort()  # smallest total size first
        candidates = [(c[3], c[4], "flac") for c in flac_candidates]
        log.info("No MP3/M4A found — falling back to FLAC (%d sources) for %s – %s",
                 len(flac_candidates), artist, album)
    else:
        log.warning("No source with %d+ tracks found for %r", min_tracks, query)
        set_grab(lidarr_id, artist, album, "failed")
        return

    for username, files, qual in candidates[:8]:
        payload = [{"filename": f["filename"], "size": f.get("size", 0)} for f in files]
        try:
            try:
                existing = set(e.name for e in os.scandir(DOWNLOADS_PATH) if e.is_dir())
            except Exception:
                existing = set()
            slskd(f"/transfers/downloads/{urllib.parse.quote(username)}", "POST", payload)
            log.info("Queued %d/%d tracks from %s (%s) — %s – %s",
                     len(files), expected, username, qual, artist, album)
            set_grab(lidarr_id, artist, album, "downloading",
                     source=username, files=[f["filename"] for f in files],
                     dl_started=time.time(), known_folders=list(existing))
            return
        except urllib.error.HTTPError as e:
            log.debug("%s rejected (%d) — trying next candidate", username, e.code)
            continue
        except Exception as e:
            log.debug("%s queue error: %s — trying next candidate", username, e)
            continue

    log.warning("All sources rejected: %s – %s", artist, album)
    set_grab(lidarr_id, artist, album, "failed")


# ── command watcher ───────────────────────────────────────────────────────────

def process_commands():
    """Check Lidarr command queue for new AlbumSearch / ArtistSearch commands."""
    last_id = int(get_state("last_command_id", 0))

    try:
        commands = lidarr("/command")
    except Exception as e:
        log.debug("Command poll failed: %s", e)
        return

    new_max = last_id
    for cmd in commands:
        cmd_id   = cmd.get("id", 0)
        cmd_name = cmd.get("name", "")
        trigger  = cmd.get("trigger", "")
        new_max  = max(new_max, cmd_id)

        if cmd_id <= last_id:
            continue

        body = cmd.get("body", {})

        if cmd_name == "AlbumSearch":
            for album_id in body.get("albumIds", []):
                if get_grab_status(album_id) in ("searching", "downloading", "imported"):
                    continue
                try:
                    alb    = lidarr(f"/album/{album_id}")
                    artist = alb.get("artist", {}).get("artistName", "")
                    title  = alb.get("title", "")
                    stats = alb.get("statistics", {})
                    if stats.get("trackFileCount", 0) >= stats.get("totalTrackCount", 1):
                        log.info("Skipping AlbumSearch — already in library: %s – %s", artist, title)
                        continue
                    if artist and title:
                        search_and_grab(album_id, artist, title)
                        time.sleep(3)
                except Exception as e:
                    log.warning("Album lookup %d failed: %s", album_id, e)

        elif cmd_name == "ArtistSearch":
            artist_id = body.get("artistId")
            if not artist_id:
                continue
            try:
                albums = lidarr(f"/album?artistId={artist_id}&monitored=true")
                for alb in albums:
                    alb_id = alb.get("id")
                    if not alb_id:
                        continue
                    if get_grab_status(alb_id) in ("searching", "downloading", "imported"):
                        continue
                    if alb.get("statistics", {}).get("trackFileCount", 0) > 0:
                        continue
                    artist = alb.get("artist", {}).get("artistName", "")
                    title  = alb.get("title", "")
                    if artist and title:
                        search_and_grab(alb_id, artist, title)
                        time.sleep(3)
            except Exception as e:
                log.warning("ArtistSearch handler failed: %s", e)

    if new_max > last_id:
        set_state("last_command_id", new_max)


# ── download monitor ──────────────────────────────────────────────────────────

def _check_format(folder_path):
    """Return (ok, reason). Rejects WAV/AIFF; allows FLAC as fallback."""
    from mutagen import File as MutagenFile
    REJECT_EXTS = {".wav", ".aiff"}
    AUDIO_EXTS  = {".mp3", ".m4a", ".flac", ".wav", ".ogg", ".aiff"}
    try:
        names = os.listdir(folder_path)
    except Exception:
        return False, "cannot list folder"
    audio = [n for n in names if os.path.splitext(n)[1].lower() in AUDIO_EXTS]
    if not audio:
        return False, "no audio files"
    bad = [n for n in audio if os.path.splitext(n)[1].lower() in REJECT_EXTS]
    if bad:
        return False, f"contains WAV/AIFF ({len(bad)} files)"
    mp3s = [n for n in audio if n.lower().endswith(".mp3")]
    if mp3s:
        brs = []
        for fname in mp3s[:5]:
            try:
                t = MutagenFile(os.path.join(folder_path, fname))
                if t and hasattr(t, "info") and hasattr(t.info, "bitrate"):
                    brs.append(t.info.bitrate // 1000)
            except Exception:
                pass
        if brs:
            avg = sum(brs) / len(brs)
            if avg < MIN_MP3_BITRATE:
                return False, f"MP3 bitrate too low ({avg:.0f}kbps, need {MIN_MP3_BITRATE}+)"
    return True, "ok"


def _flatten_multidisc(folder_path):
    """Move audio from CD N / Disc N subdirs into parent folder. Returns count moved."""
    import re, shutil
    disc_re = re.compile(r'^(?:cd|dis[ck])[\s\-_]*\d+$', re.I)
    AUDIO_EXTS = {".mp3", ".m4a", ".flac", ".ogg", ".wav"}
    moved = 0
    try:
        subdirs = [d for d in os.listdir(folder_path)
                   if os.path.isdir(os.path.join(folder_path, d)) and disc_re.match(d)]
    except Exception:
        return 0
    for sub in subdirs:
        sub_path = os.path.join(folder_path, sub)
        for fname in os.listdir(sub_path):
            if os.path.splitext(fname)[1].lower() in AUDIO_EXTS:
                src = os.path.join(sub_path, fname)
                dst = os.path.join(folder_path, fname)
                if not os.path.exists(dst):
                    shutil.move(src, dst)
                    moved += 1
        try:
            os.rmdir(sub_path)
        except Exception:
            pass
    if moved:
        log.info("Flattened %d tracks from disc subdirs in %s", moved, folder_path)
    return moved


def _regroup_disc_folders(downloads_path):
    """Consolidate orphaned CD N / Disc N folders at the downloads root by (artist, album) tag."""
    import re, shutil
    from mutagen import File as MutagenFile
    disc_re   = re.compile(r'^(?:cd|dis[ck])[\s\-_]*\d+$', re.I)
    AUDIO_EXTS = {".mp3", ".m4a", ".flac", ".ogg", ".wav"}
    try:
        disc_dirs = [e for e in os.scandir(downloads_path)
                     if e.is_dir() and disc_re.match(e.name)]
    except Exception:
        return
    if not disc_dirs:
        return
    groups = {}
    for entry in disc_dirs:
        audio = sorted(f for f in os.listdir(entry.path)
                       if os.path.splitext(f)[1].lower() in AUDIO_EXTS)
        if not audio:
            continue
        try:
            tags = MutagenFile(os.path.join(entry.path, audio[0]), easy=True)
            if not tags:
                continue
            artist = (tags.get("albumartist") or tags.get("artist") or [""])[0].strip()
            album  = (tags.get("album") or [""])[0].strip()
        except Exception:
            continue
        if not artist or not album:
            continue
        key = (artist.lower(), album.lower())
        groups.setdefault(key, {"artist": artist, "album": album, "dirs": []})
        groups[key]["dirs"].append(entry)
    for key, info in groups.items():
        artist, album = info["artist"], info["album"]
        safe    = re.sub(r'[\\/:*?"<>|]', "_", f"{artist} - {album}")
        staging = os.path.join(downloads_path, safe)
        os.makedirs(staging, exist_ok=True)
        total = 0
        for entry in info["dirs"]:
            for fname in os.listdir(entry.path):
                if os.path.splitext(fname)[1].lower() in AUDIO_EXTS:
                    src = os.path.join(entry.path, fname)
                    dst = os.path.join(staging, fname)
                    if not os.path.exists(dst):
                        shutil.move(src, dst)
                        total += 1
            try:
                shutil.rmtree(entry.path)
            except Exception:
                pass
        log.info("Regrouped %d tracks from disc folders → %s", total, staging)


def _detect_compilation(folder_path):
    """Return True if folder has ≥3 tracks that share one album but have distinct per-track artists.

    Albums where all tracks agree on a consistent non-various albumartist are never compilations,
    even if individual track artist fields vary (e.g. featured guests).
    """
    from mutagen import File as MutagenFile
    AUDIO_EXTS = {".mp3", ".m4a", ".flac", ".ogg"}
    VARIOUS = {"various artists", "va", "v/a", "v.a.", "various"}
    try:
        files = sorted(f for f in os.listdir(folder_path)
                       if os.path.splitext(f)[1].lower() in AUDIO_EXTS)
    except Exception:
        return False
    if len(files) < 3:
        return False
    artists = []
    albums  = set()
    album_artists = set()
    for fname in files:
        try:
            tags = MutagenFile(os.path.join(folder_path, fname), easy=True)
            if not tags:
                continue
            aa  = (tags.get("albumartist") or [""])[0].strip().lower()
            if aa in VARIOUS:
                return True  # already flagged as various
            if aa:
                album_artists.add(aa)
            a   = (tags.get("artist") or [""])[0].strip()
            alb = (tags.get("album")  or [""])[0].strip()
            if alb:
                albums.add(alb.lower())
            if a:
                artists.append(a.lower())
        except Exception:
            continue
    if not artists or len(albums) != 1:
        return False
    # If all tracks share a single consistent albumartist, it's not a compilation
    if len(album_artists) == 1:
        return False
    return len(set(artists)) / len(artists) > 0.5


def _fix_compilation_tags(folder_path):
    """Set albumartist=Various Artists and compilation flag on all audio files. Returns count fixed."""
    from mutagen.id3 import ID3, TPE2, TCMP, ID3NoHeaderError
    from mutagen.mp4 import MP4
    from mutagen.flac import FLAC
    AUDIO_EXTS = {".mp3", ".m4a", ".flac", ".ogg"}
    fixed = 0
    for fname in os.listdir(folder_path):
        ext  = os.path.splitext(fname)[1].lower()
        if ext not in AUDIO_EXTS:
            continue
        path = os.path.join(folder_path, fname)
        try:
            if ext == ".mp3":
                try:
                    tags = ID3(path)
                except ID3NoHeaderError:
                    tags = ID3()
                tags["TPE2"] = TPE2(encoding=3, text=["Various Artists"])
                tags["TCMP"] = TCMP(encoding=3, text=["1"])
                tags.save(path)
            elif ext == ".m4a":
                tags = MP4(path)
                tags.tags["aART"] = ["Various Artists"]
                tags.tags["cpil"] = True
                tags.save()
            elif ext == ".flac":
                tags = FLAC(path)
                tags["albumartist"] = ["Various Artists"]
                tags["compilation"] = ["1"]
                tags.save()
            fixed += 1
        except Exception:
            pass
    return fixed


def _fix_metadata(dest_folder, artist, album, lidarr_id):
    """Auto-fix bad metadata using MusicBrainz. Silently fixes; notifies only on failure."""
    import re, subprocess
    from mutagen import File as MutagenFile

    try:
        audio_files = sorted([
            f for f in os.listdir(dest_folder)
            if f.lower().endswith((".mp3", ".flac", ".m4a", ".ogg"))
        ])
    except Exception:
        return

    bad_files = []
    for fname in audio_files:
        path = os.path.join(dest_folder, fname)
        try:
            tags = MutagenFile(path, easy=True)
            if not tags:
                bad_files.append(fname)
                continue
            title = (tags.get("title") or [""])[0].strip()
            tn = (tags.get("tracknumber") or ["0"])[0].split("/")[0].strip()
            if not title or tn in ("0", ""):
                bad_files.append(fname)
        except Exception:
            pass

    if not bad_files:
        return

    log.info("Bad metadata in %s – %s (%d/%d files), attempting auto-fix",
             artist, album, len(bad_files), len(audio_files))

    if not lidarr_id:
        notify(f"Bad metadata: {artist} – {album}",
               f"{len(bad_files)}/{len(audio_files)} tracks need fixing but album not in Lidarr")
        return

    mb_release_id = None
    try:
        alb = lidarr(f"/album/{lidarr_id}")
        for rel in alb.get("releases", []):
            rid = rel.get("foreignReleaseId", "")
            if rid:
                mb_release_id = rid
                break
    except Exception as e:
        log.warning("Could not get MB release ID for album %d: %s", lidarr_id, e)

    if not mb_release_id:
        notify(f"Metadata fix failed: {artist} – {album}", "No MusicBrainz release ID in Lidarr")
        return

    mb_tracks = {}
    try:
        req = urllib.request.Request(
            f"https://musicbrainz.org/ws/2/release/{mb_release_id}?inc=recordings&fmt=json",
            headers={"User-Agent": "lidarr-slskd/1.0 (gtobrien@pm.me)"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        for medium in data.get("media", []):
            for track in medium.get("tracks", []):
                try:
                    pos = int(track.get("position") or track.get("number", 0))
                except (TypeError, ValueError):
                    continue
                title = (track.get("title") or
                         track.get("recording", {}).get("title", "")).strip()
                if pos and title:
                    mb_tracks[pos] = title
    except Exception as e:
        log.warning("MusicBrainz fetch failed for %s: %s", mb_release_id, e)
        notify(f"Metadata fix failed: {artist} – {album}", f"MusicBrainz error: {e}")
        return

    if not mb_tracks:
        notify(f"Metadata fix failed: {artist} – {album}",
               f"No tracks found in MB release {mb_release_id}")
        return

    fixed = 0
    unfixed = []
    for fname in audio_files:
        path = os.path.join(dest_folder, fname)
        m = re.match(r'^0*(\d+)', fname)
        if not m:
            continue
        track_num = int(m.group(1))
        mb_title = mb_tracks.get(track_num)
        if not mb_title:
            continue
        try:
            tags = MutagenFile(path, easy=True)
            if tags is None:
                unfixed.append(fname)
                continue
            tags["title"] = mb_title
            tags["tracknumber"] = str(track_num)
            tags["artist"] = artist
            tags["albumartist"] = artist
            tags["album"] = album
            tags.save()
            log.info("Fixed: %s → track %d %r", fname, track_num, mb_title)
            fixed += 1
        except Exception as e:
            log.warning("Tag write failed for %s: %s", fname, e)
            unfixed.append(fname)

    if fixed:
        log.info("Auto-fixed %d/%d tracks in %s – %s", fixed, len(audio_files), artist, album)
    if unfixed:
        notify(f"Metadata fix partial: {artist} – {album}",
               f"Fixed {fixed}/{len(audio_files)}, could not fix: {', '.join(unfixed[:3])}")


def _check_untagged(folder, artist, album):
    """Notify if audio files still lack artist/album tags after metadata fix."""
    from mutagen import File as MutagenFile
    AUDIO_EXTS = {".mp3", ".m4a", ".flac", ".ogg"}
    try:
        files = [f for f in os.listdir(folder) if os.path.splitext(f)[1].lower() in AUDIO_EXTS]
    except Exception:
        return
    if not files:
        return
    untagged = []
    for fname in files:
        try:
            tags = MutagenFile(os.path.join(folder, fname), easy=True)
            a   = (tags.get("artist") or [""])[0].strip() if tags else ""
            alb = (tags.get("album")  or [""])[0].strip() if tags else ""
            if not a or not alb:
                untagged.append(fname)
        except Exception:
            untagged.append(fname)
    if untagged:
        log.warning("Untagged after import: %s – %s (%d/%d files)", artist, album, len(untagged), len(files))
        notify(f"Untagged import: {artist} – {album}",
               f"{len(untagged)}/{len(files)} files missing tags — manual fix needed")


def _navidrome_stamp_created(artist, album):
    """Poll Navidrome until the album is indexed, then set created_at to now."""
    if not NAVIDROME_DB_PATH or not os.path.exists(NAVIDROME_DB_PATH):
        return
    if not NAVIDROME_USER:
        return
    _navidrome_scan()
    album_id = None
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            url = (f"{NAVIDROME_URL}/rest/search3.view"
                   f"?u={urllib.parse.quote(NAVIDROME_USER)}"
                   f"&p={urllib.parse.quote(NAVIDROME_PASS)}"
                   f"&v=1.15.0&c=lidarr-slskd&f=json"
                   f"&query={urllib.parse.quote(album)}&albumCount=10")
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
            for a in data.get("subsonic-response", {}).get("searchResult3", {}).get("album", []):
                if (a.get("artist", "").lower() == artist.lower() and
                        a.get("name", "").lower() == album.lower()):
                    album_id = a["id"]
                    break
        except Exception:
            pass
        if album_id:
            break
        time.sleep(10)
    if not album_id:
        log.warning("stamp_created: %s – %s not found in Navidrome after 2min", artist, album)
        return
    try:
        conn = sqlite3.connect(NAVIDROME_DB_PATH)
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn.execute("UPDATE album SET created_at=? WHERE id=?", (now, album_id))
        conn.commit()
        conn.close()
        log.info("Stamped Navidrome created_at=%s for %s – %s", now, artist, album)
    except Exception as e:
        log.warning("stamp_created: DB update failed: %s", e)


def import_folder(lidarr_id, artist, album, dl_folder):
    """
    Move downloaded files directly into the music library, then trigger a Lidarr rescan.
    Returns number of files moved.
    """
    import shutil, re

    # Flatten any CD N / Disc N subdirs before format check
    _flatten_multidisc(dl_folder)

    ok, reason = _check_format(dl_folder)
    if not ok:
        log.warning("import_folder: rejecting %s — %s", dl_folder, reason)
        return 0

    # Fix compilation tags before routing
    if _detect_compilation(dl_folder):
        n = _fix_compilation_tags(dl_folder)
        log.info("import_folder: detected compilation in %s — fixed tags on %d files", dl_folder, n)

    # Ask Lidarr for the album's release year
    year = ""
    try:
        alb = lidarr(f"/album/{lidarr_id}")
        rd = alb.get("releaseDate") or alb.get("releases", [{}])[0].get("releaseDate", "")
        year = rd[:4] if rd else ""
    except Exception:
        pass

    # Build destination: /music/<Artist Name>/<Album Title> (<Year>)/
    album_folder = f"{album} ({year})" if year else album
    # Use the artist's configured path from Lidarr if available
    artist_path = None
    artist_id = None
    try:
        art = lidarr(f"/artist?artistName={urllib.parse.quote(artist)}")
        for a in art:
            if a.get("artistName", "").lower() == artist.lower():
                artist_path = a.get("path", "").replace("/music", MUSIC_PATH, 1)
                artist_id = a.get("id")
                break
    except Exception:
        pass

    if not artist_path:
        artist_path = os.path.join(MUSIC_PATH, artist)

    dest = os.path.join(artist_path, album_folder)
    os.makedirs(dest, exist_ok=True)

    # Abort if the destination already contains audio — album already imported
    AUDIO_EXTS_CHECK = {".mp3", ".m4a", ".flac", ".ogg", ".wav"}
    existing_audio = [f for f in os.listdir(dest)
                      if os.path.splitext(f)[1].lower() in AUDIO_EXTS_CHECK]
    if existing_audio:
        log.warning("import_folder: destination already has %d audio file(s) — skipping to avoid duplicate: %s",
                    len(existing_audio), dest)
        try:
            shutil.rmtree(dl_folder)
        except Exception:
            pass
        return 0

    # Move all audio files from download folder to dest
    moved = 0
    for fname in os.listdir(dl_folder):
        if fname.lower().endswith((".mp3", ".m4a", ".ogg", ".flac")):
            shutil.move(os.path.join(dl_folder, fname), os.path.join(dest, fname))
            moved += 1

    if moved:
        # Remove now-empty download folder
        try:
            shutil.rmtree(dl_folder)
        except Exception:
            pass
        # Trigger Lidarr rescan, wait for completion, then rename
        lidarr_artist_path = artist_path.replace(MUSIC_PATH, "/music", 1)
        try:
            cmd = lidarr("/command", "POST", {"name": "RescanFolders", "folders": [lidarr_artist_path]})
            cmd_id = cmd.get("id")
            # Poll until rescan finishes so RenameArtist sees the new files
            if cmd_id:
                for _ in range(30):
                    time.sleep(2)
                    status = lidarr(f"/command/{cmd_id}").get("status", "")
                    if status in ("completed", "failed"):
                        break
        except Exception as e:
            log.warning("Rescan trigger failed: %s", e)

        if artist_id:
            try:
                lidarr("/command", "POST", {"name": "RenameArtist", "artistIds": [artist_id]})
                log.info("Queued Lidarr rename for artist %d", artist_id)
            except Exception as e:
                log.warning("Rename trigger failed: %s", e)

        log.info("Moved %d files to %s", moved, dest)
        _fix_mb_albumid(dest)
        _fix_metadata(dest, artist, album, lidarr_id)
        _check_untagged(dest, artist, album)
        _normalize_genres(dest)
        _fetch_genres_lastfm(dest, artist)
        _fix_label(dest)
        _fetch_cover_art(dest, artist, album, lidarr_id)
        _navidrome_stamp_created(artist, album)

    return moved


def _fix_mb_albumid(folder):
    """Stamp the most common non-empty MB album ID onto any tracks missing it."""
    from mutagen import File as MutagenFile
    from collections import Counter
    AUDIO = {".mp3", ".m4a", ".flac", ".ogg"}
    try:
        files = [os.path.join(folder, f) for f in os.listdir(folder)
                 if os.path.splitext(f)[1].lower() in AUDIO]
    except Exception:
        return
    if not files:
        return
    ids = []
    for f in files:
        try:
            t = MutagenFile(f, easy=True)
            ids.append((t.get("musicbrainz_albumid") or [""])[0].strip() if t else "")
        except Exception:
            ids.append("")
    non_empty = [i for i in ids if i]
    if not non_empty:
        return
    canonical = Counter(non_empty).most_common(1)[0][0]
    for fpath, existing in zip(files, ids):
        if existing == canonical:
            continue
        ext = os.path.splitext(fpath)[1].lower()
        try:
            if ext == ".mp3":
                from mutagen.id3 import ID3, TXXX
                t = ID3(fpath)
                t.delall("TXXX:MusicBrainz Album Id")
                t.add(TXXX(encoding=3, desc="MusicBrainz Album Id", text=[canonical]))
                t.save()
            elif ext == ".m4a":
                from mutagen.mp4 import MP4
                t = MP4(fpath)
                t.tags["----:com.apple.iTunes:MusicBrainz Album Id"] = [canonical.encode()]
                t.save()
            elif ext == ".flac":
                from mutagen.flac import FLAC
                t = FLAC(fpath)
                t["musicbrainz_albumid"] = [canonical]
                t.save()
            log.info("MB album ID stamped: %s", os.path.basename(fpath))
        except Exception as e:
            log.warning("MB album ID stamp failed for %s: %s", os.path.basename(fpath), e)


def _normalize_genres(folder):
    script = "/scripts/navidrome/normalize_genres.py"
    if not os.path.exists(script):
        log.warning("normalize_genres.py not found at %s", script)
        return
    try:
        result = subprocess.run(
            [sys.executable, script, "--apply", "--folder", folder],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            log.info("Genre normalization complete for %s", folder)
        else:
            log.warning("Genre normalization failed for %s: %s", folder, result.stderr[:500])
    except Exception as e:
        log.warning("Genre normalization error for %s: %s", folder, e)


def _fetch_genres_lastfm(folder, artist):
    """If no audio files in folder have genre tags, fetch from Last.fm and normalize."""
    import json as _json
    import urllib.parse as _urlparse
    import urllib.request as _urlreq

    LASTFM_KEY = "ef955e9f302f8671671c27052ea417f1"
    AUDIO_EXTS = {".mp3", ".m4a", ".flac", ".ogg", ".wav"}
    NOISE = {
        "seen live", "albums i own", "favorite", "favourites", "love", "loved",
        "my favorites", "my favourite albums", "classic", "best", "good", "great",
        "amazing", "beautiful", "awesome", "excellent", "brilliant", "masterpiece",
        "00s", "10s", "20s", "60s", "70s", "80s", "90s", "2000s", "2010s",
    }

    try:
        from mutagen import File as MutagenFile
        from mutagen.id3 import ID3, TCON
        from mutagen.mp4 import MP4
        from mutagen.flac import FLAC
    except ImportError:
        return

    try:
        files = [os.path.join(folder, f) for f in os.listdir(folder)
                 if os.path.splitext(f)[1].lower() in AUDIO_EXTS]
    except Exception:
        return
    if not files:
        return

    # Check if any file already has a genre tag
    for fpath in files:
        try:
            t = MutagenFile(fpath, easy=True)
            if t and t.get("genre"):
                return  # already tagged — normalize_genres handled it
        except Exception:
            pass

    # Look up artist tags on Last.fm
    artist_clean = re.split(r'\s+[&x+]\s+|\s*/\s*', artist)[0].strip()
    tags = []
    for name in [artist_clean, artist]:
        params = _urlparse.urlencode({
            "method": "artist.getinfo", "artist": name,
            "api_key": LASTFM_KEY, "format": "json",
        })
        try:
            req = _urlreq.Request(
                f"https://ws.audioscrobbler.com/2.0/?{params}",
                headers={"User-Agent": "MusicLibraryGenreFixer/1.0"},
            )
            with _urlreq.urlopen(req, timeout=10) as r:
                data = _json.loads(r.read())
            raw = data.get("artist", {}).get("tags", {}).get("tag", [])
            if isinstance(raw, dict):
                raw = [raw]
            tags = [t["name"] for t in raw
                    if isinstance(t, dict) and t.get("name")
                    and t["name"].lower() not in NOISE
                    and not t["name"].isdigit()][:3]
            if tags:
                break
        except Exception as e:
            log.debug("Last.fm lookup failed for %s: %s", name, e)

    if not tags:
        log.info("Last.fm: no genre tags found for %s", artist)
        return

    genre_str = " / ".join(tags)
    log.info("Last.fm genre fetch: %s → %s", artist, genre_str)

    # Write raw tags then re-run normalize_genres to canonicalize
    written = 0
    for fpath in files:
        ext = os.path.splitext(fpath)[1].lower()
        try:
            if ext == ".mp3":
                t = ID3(fpath); t.delall("TCON"); t.add(TCON(encoding=3, text=[genre_str])); t.save()
            elif ext == ".m4a":
                t = MP4(fpath); t["\xa9gen"] = [genre_str]; t.save()
            elif ext == ".flac":
                t = FLAC(fpath); t["genre"] = [genre_str]; t.save()
            else:
                t = MutagenFile(fpath, easy=True)
                if t: t["genre"] = [genre_str]; t.save()
            written += 1
        except Exception as e:
            log.warning("Last.fm genre write failed for %s: %s", os.path.basename(fpath), e)

    if written:
        _normalize_genres(folder)  # canonicalize the raw Last.fm tags


def _fix_label(folder):
    script = "/scripts/navidrome/fix_missing_label.py"
    if not os.path.exists(script):
        return
    if not DISCOGS_TOKEN:
        return
    try:
        result = subprocess.run(
            [sys.executable, script, "--folder", folder, "--token", DISCOGS_TOKEN],
            capture_output=True, text=True, timeout=30
        )
        out = (result.stdout or result.stderr or "").strip()
        if out:
            log.info("Label fix: %s", out)
    except Exception as e:
        log.warning("Label fix error for %s: %s", folder, e)


def _fetch_cover_art(dest_folder, artist, album, lidarr_id):
    """Download cover.jpg if missing. Tries MusicBrainz CAA first, then Discogs."""
    cover = os.path.join(dest_folder, "cover.jpg")
    if os.path.exists(cover):
        return

    # Get MB release ID from audio file tags first
    from mutagen import File as MutagenFile
    AUDIO_EXTS = {".mp3", ".m4a", ".flac", ".ogg"}
    mb_release_id = None
    try:
        for fname in sorted(os.listdir(dest_folder)):
            if os.path.splitext(fname)[1].lower() in AUDIO_EXTS:
                t = MutagenFile(os.path.join(dest_folder, fname), easy=True)
                if t:
                    mbid = (t.get("musicbrainz_albumid") or [""])[0].strip()
                    if mbid:
                        mb_release_id = mbid
                        break
    except Exception:
        pass

    # Fall back to Lidarr's stored release ID
    if not mb_release_id and lidarr_id:
        try:
            alb = lidarr(f"/album/{lidarr_id}")
            for rel in alb.get("releases", []):
                rid = rel.get("foreignReleaseId", "")
                if rid:
                    mb_release_id = rid
                    break
        except Exception:
            pass

    if mb_release_id:
        try:
            caa_req = urllib.request.Request(
                f"https://coverartarchive.org/release/{mb_release_id}/front",
                headers={"User-Agent": "lidarr-slskd/1.0 (gtobrien@pm.me)"},
            )
            with urllib.request.urlopen(caa_req, timeout=20) as r:
                data = r.read()
            if data:
                with open(cover, "wb") as f:
                    f.write(data)
                log.info("Cover art saved from CAA for %s – %s", artist, album)
                return
        except Exception as e:
            log.debug("CAA cover fetch failed for %s: %s", mb_release_id, e)

    if not DISCOGS_TOKEN:
        log.info("No cover art found for %s – %s (no Discogs token)", artist, album)
        return
    try:
        params = urllib.parse.urlencode({
            "q": f"{artist} {album}", "type": "release", "format": "album",
        })
        search_req = urllib.request.Request(
            f"https://api.discogs.com/database/search?{params}",
            headers={
                "Authorization": f"Discogs token={DISCOGS_TOKEN}",
                "User-Agent": "lidarr-slskd/1.0 (gtobrien@pm.me)",
            }
        )
        with urllib.request.urlopen(search_req, timeout=15) as r:
            results = json.loads(r.read()).get("results", [])
        image_url = next(
            (res.get("cover_image") for res in results[:5]
             if res.get("cover_image") and "spacer" not in res.get("cover_image", "")),
            None
        )
        if not image_url:
            log.info("No Discogs cover found for %s – %s", artist, album)
            return
        img_req = urllib.request.Request(
            image_url,
            headers={
                "Authorization": f"Discogs token={DISCOGS_TOKEN}",
                "User-Agent": "lidarr-slskd/1.0 (gtobrien@pm.me)",
            }
        )
        with urllib.request.urlopen(img_req, timeout=20) as r:
            img_data = r.read()
        if img_data:
            with open(cover, "wb") as f:
                f.write(img_data)
            log.info("Cover art saved from Discogs for %s – %s", artist, album)
    except Exception as e:
        log.warning("Discogs cover fetch failed for %s – %s: %s", artist, album, e)


def find_new_download_folders(known_folders_json, since_epoch=None):
    """Return folders in DOWNLOADS_PATH that didn't exist when the grab was queued.

    Uses the snapshot of folder names taken before the download started.
    Falls back to mtime comparison if no snapshot is available.
    """
    try:
        current = {e.name: e for e in os.scandir(DOWNLOADS_PATH) if e.is_dir()}
    except Exception as e:
        log.warning("Could not scan downloads dir: %s", e)
        return []

    if known_folders_json:
        known = set(json.loads(known_folders_json))
        new = [name for name in current if name not in known]
        return new

    # Fallback: mtime-based (original behaviour, kept for backwards compat)
    if since_epoch:
        return [name for name, e in current.items() if e.stat().st_mtime >= since_epoch]
    return []


def check_downloads():
    c = db()
    active = c.execute("SELECT * FROM grabs WHERE status='downloading'").fetchall()
    c.close()
    if not active:
        return

    try:
        transfers = slskd("/transfers/downloads")
    except Exception:
        return

    file_state = {}
    for user in transfers:
        for d in user.get("directories", []):
            for f in d.get("files", []):
                file_state[f["filename"]] = f.get("state", "")

    done_markers = {"Succeeded", "Rejected", "Errored", "Cancelled", "TimedOut", "Aborted"}

    for row in active:
        # Timeout: if the grab has been downloading too long, blacklist source and retry
        if row["dl_started"] and (time.time() - row["dl_started"]) > DOWNLOAD_TIMEOUT:
            log.warning("Download timed out after %ds: %s – %s", DOWNLOAD_TIMEOUT, row["artist"], row["album"])
            if row["source"]:
                record_failure(row["lidarr_id"], row["source"])
            set_grab(row["lidarr_id"], row["artist"], row["album"], "failed")
            search_and_grab(row["lidarr_id"], row["artist"], row["album"])
            continue

        queued    = json.loads(row["files"] or "[]")
        states    = [file_state.get(fn, "InProgress") for fn in queued]
        finished  = [s for s in states if any(m in s for m in done_markers)]
        succeeded = [s for s in states if "Succeeded" in s]
        rejected  = [s for s in states if "Rejected" in s]

        # Early bail: if more than half the queued files are already rejected, don't wait
        if len(rejected) > len(queued) / 2:
            log.warning("Source %s rejected %d/%d files — blacklisting and retrying: %s – %s",
                        row["source"], len(rejected), len(queued), row["artist"], row["album"])
            if row["source"]:
                record_failure(row["lidarr_id"], row["source"])
            set_grab(row["lidarr_id"], row["artist"], row["album"], "failed")
            search_and_grab(row["lidarr_id"], row["artist"], row["album"])
            continue

        if len(finished) < len(queued):
            continue

        expected_count = len(queued)
        if succeeded and len(succeeded) < expected_count:
            log.warning("Incomplete download (%d/%d succeeded): %s – %s — blacklisting source and retrying",
                        len(succeeded), expected_count, row["artist"], row["album"])
            if row["source"]:
                record_failure(row["lidarr_id"], row["source"])
            set_grab(row["lidarr_id"], row["artist"], row["album"], "failed")
            search_and_grab(row["lidarr_id"], row["artist"], row["album"])
            continue

        if succeeded:
            since = row["dl_started"] or (time.time() - 3600)
            new_folders = find_new_download_folders(row["known_folders"], since_epoch=since)
            imported = 0
            for folder_name in new_folders:
                dl_folder = os.path.join(DOWNLOADS_PATH, folder_name)
                try:
                    n = import_folder(row["lidarr_id"], row["artist"], row["album"], dl_folder)
                    imported += n
                except Exception as e:
                    log.warning("Import failed for %s: %s", folder_name, e)
            if imported == 0:
                log.warning("Import returned 0 tracks for %s – %s (folders checked: %s)",
                            row["artist"], row["album"], new_folders)
            notify(
                f"{row['artist']} – {row['album']}",
                f"Downloaded via {row['source']} ({imported} tracks imported)"
            )
            set_grab(row["lidarr_id"], row["artist"], row["album"], "imported")
        else:
            log.warning("All files rejected: %s – %s", row["artist"], row["album"])
            set_grab(row["lidarr_id"], row["artist"], row["album"], "failed")


def _navidrome_scan():
    if not NAVIDROME_USER:
        return
    try:
        url = (f"{NAVIDROME_URL}/rest/startScan"
               f"?u={urllib.parse.quote(NAVIDROME_USER)}"
               f"&p={urllib.parse.quote(NAVIDROME_PASS)}"
               f"&v=1.16.1&c=lidarr-slskd&f=json&fullScan=true")
        urllib.request.urlopen(url, timeout=10)
    except Exception as e:
        log.warning("Navidrome scan trigger failed: %s", e)


def _beet_import(folder_path):
    """Try beets auto-tag import. Returns True if all audio files were moved."""
    import shutil
    AUDIO_EXTS = {".mp3", ".flac", ".m4a", ".ogg"}

    def _count_audio():
        try:
            return sum(1 for f in os.listdir(folder_path)
                       if os.path.splitext(f)[1].lower() in AUDIO_EXTS)
        except FileNotFoundError:
            return 0

    before = _count_audio()
    if not before:
        return False

    try:
        result = subprocess.run(
            ["beet", "-c", BEETS_CONFIG, "import", "-A", folder_path],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode not in (0, 1):
            log.warning("beets exited %d for %s: %s",
                        result.returncode, os.path.basename(folder_path), result.stderr[:300])
            return False
    except subprocess.TimeoutExpired:
        log.warning("beets timed out for %s", os.path.basename(folder_path))
        return False
    except FileNotFoundError:
        log.warning("beets not found — skipping auto-tag")
        return False
    except Exception as e:
        log.warning("beets error for %s: %s", os.path.basename(folder_path), e)
        return False

    after = _count_audio()
    if after == 0:
        try:
            shutil.rmtree(folder_path)
        except Exception:
            pass
        log.info("beets: imported %s (%d files)", os.path.basename(folder_path), before)
        return True

    if after < before:
        log.warning("beets: partial import for %s (%d/%d files moved) — falling back",
                    os.path.basename(folder_path), before - after, before)
    else:
        log.info("beets: no confident match for %s — falling back", os.path.basename(folder_path))
    return False


# ── manual download sweeper ───────────────────────────────────────────────────

def _evict_junk(downloads_path):
    """Remove empty directories and single-track orphan folders from downloads."""
    AUDIO_EXTS = {".mp3", ".m4a", ".flac", ".ogg", ".wav"}
    try:
        entries = [e for e in os.scandir(downloads_path) if e.is_dir()]
    except Exception:
        return
    for entry in entries:
        try:
            contents = os.listdir(entry.path)
        except Exception:
            continue
        if not contents:
            os.rmdir(entry.path)
            log.info("Evicted empty dir: %s", entry.name)
            continue
        audio = [f for f in contents if os.path.splitext(f)[1].lower() in AUDIO_EXTS]
        if len(audio) == 1:
            import shutil
            shutil.rmtree(entry.path)
            log.info("Evicted single-track orphan: %s", entry.name)


def sweep_manual_downloads():
    """Import completed folders in DOWNLOADS_PATH that weren't queued by lidarr-slskd."""
    from mutagen import File as MutagenFile

    _evict_junk(DOWNLOADS_PATH)

    # Consolidate any orphaned CD N / Disc N folders before processing
    _regroup_disc_folders(DOWNLOADS_PATH)

    # Build set of folder name fragments from active slskd transfers
    try:
        transfers = slskd("/transfers/downloads")
    except Exception:
        return

    active_folder_names = set()
    inactive_states = {"Succeeded", "Rejected", "Errored", "Cancelled", "TimedOut", "Aborted", "Completed"}
    for user in transfers:
        for d in user.get("directories", []):
            for f in d.get("files", []):
                if not any(s in f.get("state", "") for s in inactive_states):
                    # Extract local folder name from slskd filename path
                    parts = f["filename"].replace("\\", "/").split("/")
                    for part in parts[:-1]:  # all except filename itself
                        active_folder_names.add(part.lower())

    # Scan downloads dir for candidate folders
    try:
        entries = [e for e in os.scandir(DOWNLOADS_PATH) if e.is_dir()]
    except Exception:
        return

    now = time.time()
    for entry in entries:
        folder_path = entry.path
        folder_name = entry.name

        # Skip folders that slskd is still actively writing to
        if folder_name.lower() in active_folder_names:
            continue

        # Skip recently modified folders (still downloading or just landed).
        # Negative age means NAS clock is ahead of host — treat as old enough.
        try:
            age_secs = now - entry.stat().st_mtime
        except Exception:
            continue
        if 0 <= age_secs < 300:  # must have been sitting for 5+ minutes
            continue

        # Flatten any CD N / Disc N subdirs first
        _flatten_multidisc(folder_path)

        # Try beets auto-tag import first — handles tagging, renaming, and moving
        if _beet_import(folder_path):
            _navidrome_scan()
            notify(f"Imported: {folder_name}", "Auto-tagged and imported via beets")
            continue

        # Gate on format — reject WAV/AIFF and low-bitrate MP3 (beets fallback only)
        ok, reason = _check_format(folder_path)
        if not ok:
            log.warning("Sweep: skipping %s — %s", folder_name, reason)
            continue

        # Find audio files
        try:
            audio_files = [
                f for f in os.listdir(folder_path)
                if f.lower().endswith((".mp3", ".m4a", ".ogg", ".flac"))
            ]
        except Exception:
            continue
        if not audio_files:
            continue

        # Detect and fix compilations before routing
        if _detect_compilation(folder_path):
            n_fixed = _fix_compilation_tags(folder_path)
            log.info("Sweep: detected compilation in %s — fixed tags on %d files", folder_name, n_fixed)

        # Read tags from first audio file to identify artist/album/year
        sample = os.path.join(folder_path, sorted(audio_files)[0])
        try:
            tags = MutagenFile(sample, easy=True)
            artist = (tags.get("albumartist") or tags.get("artist") or [""])[0].strip()
            album  = (tags.get("album") or [""])[0].strip()
            year   = (tags.get("date") or tags.get("year") or [""])[0].strip()[:4]
        except Exception:
            log.warning("Sweep: could not read tags from %s", sample)
            continue

        if not artist or not album:
            log.warning("Sweep: no artist/album tags in %s — skipping", folder_name)
            continue

        log.info("Sweep: found manual download — %s – %s (%s)", artist, album, year)

        # Look up artist in Lidarr to get configured path and ID
        import shutil
        artist_path = None
        artist_id   = None
        lidarr_id   = None
        try:
            results = lidarr(f"/artist?artistName={urllib.parse.quote(artist)}")
            for a in results:
                if a.get("artistName", "").lower() == artist.lower():
                    artist_path = a.get("path", "").replace("/music", MUSIC_PATH, 1)
                    artist_id   = a.get("id")
                    break
        except Exception:
            pass

        # Try to find the album in Lidarr for rename support
        if artist_id:
            try:
                albums = lidarr(f"/album?artistId={artist_id}")
                for alb in albums:
                    if alb.get("title", "").lower() == album.lower():
                        lidarr_id = alb.get("id")
                        if not year:
                            rd = alb.get("releaseDate", "")
                            year = rd[:4] if rd else ""
                        break
            except Exception:
                pass

        if not artist_path:
            artist_path = os.path.join(MUSIC_PATH, artist)

        album_folder = f"{album} ({year})" if year else album
        dest = os.path.join(artist_path, album_folder)
        os.makedirs(dest, exist_ok=True)

        existing_audio = [f for f in os.listdir(dest)
                          if os.path.splitext(f)[1].lower() in {".mp3", ".m4a", ".flac", ".ogg", ".wav"}]
        if existing_audio:
            log.warning("Sweep: destination already has %d audio file(s) — skipping duplicate: %s",
                        len(existing_audio), dest)
            try:
                shutil.rmtree(folder_path)
            except Exception:
                pass
            continue

        moved = 0
        for fname in audio_files:
            if not fname.lower().endswith((".mp3", ".m4a", ".ogg", ".flac")):
                continue
            src = os.path.join(folder_path, fname)
            dst = os.path.join(dest, fname)
            if not os.path.exists(dst):
                shutil.move(src, dst)
                moved += 1

        if moved:
            try:
                shutil.rmtree(folder_path)
            except Exception:
                pass
            log.info("Sweep: moved %d files → %s", moved, dest)

            # Rescan then rename
            lidarr_artist_path = artist_path.replace(MUSIC_PATH, "/music", 1)
            try:
                cmd = lidarr("/command", "POST", {"name": "RescanFolders", "folders": [lidarr_artist_path]})
                cmd_id = cmd.get("id")
                if cmd_id:
                    for _ in range(30):
                        time.sleep(2)
                        if lidarr(f"/command/{cmd_id}").get("status") in ("completed", "failed"):
                            break
            except Exception as e:
                log.warning("Sweep rescan failed: %s", e)

            if artist_id:
                try:
                    lidarr("/command", "POST", {"name": "RenameArtist", "artistIds": [artist_id]})
                except Exception:
                    pass

            notify(f"{artist} – {album}", f"Manual download imported ({moved} tracks)")
            _fix_mb_albumid(dest)
            _fix_metadata(dest, artist, album, lidarr_id)
            _normalize_genres(dest)
            _fetch_genres_lastfm(dest, artist)
            _fix_label(dest)
            _fetch_cover_art(dest, artist, album, lidarr_id)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    init_db()
    # Seed the last_command_id to current max so we only act on future commands
    if not get_state("last_command_id"):
        try:
            commands = lidarr("/command")
            if commands:
                max_id = max(c.get("id", 0) for c in commands)
                set_state("last_command_id", max_id)
                log.info("Seeded command cursor at %d", max_id)
        except Exception as e:
            log.warning("Could not seed command cursor: %s", e)

    log.info("lidarr-slskd started — watching for AlbumSearch commands (poll every %ds)", POLL_SECS)
    last_sweep = 0
    while True:
        check_downloads()
        process_commands()
        if time.time() - last_sweep >= SWEEP_SECS:
            sweep_manual_downloads()
            last_sweep = time.time()
        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
