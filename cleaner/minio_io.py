import os
from dataclasses import dataclass
from typing import Optional, List

from minio import Minio


@dataclass
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    bucket: str
    silver_prefix: str = ""           # e.g. "", "silver", or "transform/silver"
    silver_filename: str = "merged.csv"  # file to fetch under the corr folder


class MinioIO:
    def __init__(self, cfg: MinioConfig):
        self.cfg = cfg
        self.client = Minio(
            endpoint=cfg.endpoint,
            access_key=cfg.access_key,
            secret_key=cfg.secret_key,
            secure=cfg.secure,
        )
        if not self.client.bucket_exists(cfg.bucket):
            raise RuntimeError(f"Bucket '{cfg.bucket}' does not exist in MinIO.")
        os.makedirs("/tmp/silver", exist_ok=True)

    def _prefixes_for_corr(self, corr_id: str) -> List[str]:
        """
        Generate a few common layouts:
          <prefix>/<corr_id>/
          <prefix>/corr=<corr_id>/
        (Handles empty prefix cleanly.)
        """
        base = self.cfg.silver_prefix.strip("/")
        parts = []
        if base:
            parts.append(f"{base}/{corr_id}/")
            parts.append(f"{base}/corr={corr_id}/")
        else:
            parts.append(f"{corr_id}/")
            parts.append(f"corr={corr_id}/")
        return parts

    def download_csv_for_corr(self, corr_id: str) -> Optional[str]:
        """
        Look for <prefix>/<corr-pattern>/{silver_filename}; if not found,
        fall back to the first *.csv under that corr folder.
        """
        candidates = self._prefixes_for_corr(corr_id)
        name = self.cfg.silver_filename

        # 1) Try exact file match first
        for pref in candidates:
            key = f"{pref}{name}"
            try:
                # stat is cheap; if it exists, download it
                self.client.stat_object(self.cfg.bucket, key)
                local = f"/tmp/silver/{corr_id}__{os.path.basename(name)}"
                self.client.fget_object(self.cfg.bucket, key, local)
                return local
            except Exception:
                pass  # not found under this pattern; try next

        # 2) Fallback: any CSV under corr folder (first one)
        for pref in candidates:
            objs = self.client.list_objects(self.cfg.bucket, prefix=pref, recursive=True)
            for o in objs:
                if o.object_name.lower().endswith(".csv"):
                    local = f"/tmp/silver/{corr_id}__{os.path.basename(o.object_name)}"
                    self.client.fget_object(self.cfg.bucket, o.object_name, local)
                    return local

        return None
