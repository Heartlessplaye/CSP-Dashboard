"""Input directory monitoring: inotify notifications (watchdog) + periodic full scan.

Notifications give low latency; the periodic scan is the safety net (missed events,
network filesystems, startup) and the source of truth for which files/sources exist.
"""

from __future__ import annotations

import fnmatch
import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class FileStat:
    inode: int
    size: int
    mtime_ns: int


class DirWatcher:
    def __init__(self, root: Path, ignore_patterns: list[str], use_watchdog: bool = True) -> None:
        self.root = Path(root)
        self.ignore_patterns = list(ignore_patterns)
        self.use_watchdog = use_watchdog
        self._lock = threading.Lock()
        self._dirty: set[str] = set()
        self._structure_changed = False
        self._observer = None

    # ----------------------------------------------------------------- lifecycle
    def start(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.use_watchdog:
            return
        try:
            from watchdog.events import FileSystemEventHandler
            from watchdog.observers import Observer
        except ImportError:  # pragma: no cover
            log.warning("watchdog not installed; using periodic scans only")
            return

        watcher = self

        class Handler(FileSystemEventHandler):
            def on_any_event(self, event):  # noqa: D401
                if event.event_type in ("opened", "closed_no_write"):
                    return
                paths = [event.src_path, getattr(event, "dest_path", "") or ""]
                with watcher._lock:
                    for p in paths:
                        if p:
                            watcher._dirty.add(os.fsdecode(p))
                    if event.is_directory or event.event_type in ("deleted", "moved", "created"):
                        watcher._structure_changed = True

        try:
            self._observer = Observer()
            self._observer.schedule(Handler(), str(self.root), recursive=True)
            self._observer.daemon = True
            self._observer.start()
        except OSError as exc:  # e.g. inotify watch limit reached
            log.warning("file notifications unavailable (%s); using periodic scans only", exc)
            self._observer = None

    def stop(self) -> None:
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None

    # ----------------------------------------------------------------- events
    def drain(self) -> tuple[set[str], bool]:
        """Paths touched since the last call, and whether files/dirs were created/removed."""
        with self._lock:
            dirty, self._dirty = self._dirty, set()
            structure, self._structure_changed = self._structure_changed, False
        return dirty, structure

    # ----------------------------------------------------------------- scanning
    def ignored(self, name: str) -> bool:
        return any(fnmatch.fnmatch(name, p) for p in self.ignore_patterns)

    def source_of(self, path: str) -> str | None:
        """Source id for a file path (first directory level under the root)."""
        try:
            rel = Path(path).relative_to(self.root)
        except ValueError:
            return None
        parts = rel.parts
        if len(parts) < 2 or any(self.ignored(p) for p in parts):
            return None
        return parts[0]

    def scan(self) -> dict[str, dict[str, FileStat]]:
        """{source: {file path: stat}} for every source folder and file under the root."""
        result: dict[str, dict[str, FileStat]] = {}
        try:
            entries = list(os.scandir(self.root))
        except FileNotFoundError:
            return result
        for entry in entries:
            if not entry.is_dir(follow_symlinks=False) or self.ignored(entry.name):
                continue
            files: dict[str, FileStat] = {}
            for dirpath, dirnames, filenames in os.walk(entry.path):
                dirnames[:] = [d for d in dirnames if not self.ignored(d)]
                for fn in filenames:
                    if self.ignored(fn):
                        continue
                    p = os.path.join(dirpath, fn)
                    try:
                        st = os.stat(p)
                    except FileNotFoundError:
                        continue
                    files[p] = FileStat(st.st_ino, st.st_size, st.st_mtime_ns)
            result[entry.name] = files
        return result

    @staticmethod
    def stat(path: str) -> FileStat | None:
        try:
            st = os.stat(path)
        except (FileNotFoundError, NotADirectoryError):
            return None
        if not os.path.isfile(path):
            return None
        return FileStat(st.st_ino, st.st_size, st.st_mtime_ns)
