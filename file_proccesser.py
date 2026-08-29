import asyncio
import hashlib
import json
import pathlib
import shutil
import zlib
from PIL import Image
from dataclasses import dataclass
from utils import Logger, Blob
try:
    import compression.zstd as zstd
    zstd_installed = True
except ImportError:
    zstd_installed = False

RAW = 0
ZLIB = 1
ZSTD = 2

class FileProcesser:
    def __init__(self, ffmpeg_ref: str | None = None, ffprobe_ref: str | None = None) -> None:
        self.ffmpeg = shutil.which(ffmpeg_ref or "ffmpeg")
        self.ffprobe = shutil.which(ffprobe_ref or "ffprobe")
        if zstd_installed:
            Logger.info_sync('Using ZStandard for compression & Decompression')
        else:
            Logger.info_sync('Using Zlib for compression & Decompression')

    async def _run_ffmpeg(self, args: list[str]):
        process = await asyncio.create_subprocess_exec(
            *args, 
            stdout=asyncio.subprocess.PIPE, 
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        return process.returncode, stdout, stderr

    @dataclass
    class CompressedBlob:
        data:bytes
        compression_type: int = RAW

    async def compress_bytes(self, data:bytes | Blob):
        c = bytes()

        if isinstance(data, Blob):
            data = data.data or bytes()

        d = self.CompressedBlob(data)

        await Logger.info_async('Compressing bytes...')

        try:
            if zstd_installed:
                c = await asyncio.to_thread(zstd.compress, data)
                d.compression_type = ZSTD
                d.data = c
            else:
                c =  await asyncio.to_thread(zlib.compress, data)
                d.compression_type = ZLIB
                d.data = c
            if len(c) > len(data):
                await Logger.warn_async(f'Compression ({len(c)} bytes) increased the file size ({len(data)} bytes); Reverting...')
                d.data = data
                d.compression_type = RAW
            else:
                await Logger.info_async(f'Data successfully compressed')
        except Exception as e: 
            await Logger.error_async(f"An error occured while compressing data: {repr(e)}")

        return d

    async def decompress_bytes(self, data: CompressedBlob):
        d = bytes()

        data_ = data.data or bytes()

        flag = int(data.compression_type)

        data = data
        await Logger.info_async('Decompressing bytes...')
        if flag == RAW:
            d += data_
        elif flag == ZLIB:
            try:
                d = await asyncio.to_thread(zlib.decompress, data_)
                await Logger.info_async(f'Data successfully decompressed')
            except Exception as e: 
                await Logger.error_async(f"An error occured while decompressing data: {repr(e)}")
        elif flag == ZSTD:
            if not zstd_installed:
                await Logger.error_async(f"ZStandard compressed blobs cannot be decompressded without the ZSTD module installed. Aborting..")
                return d
            else:
                d = await asyncio.to_thread(zstd.decompress, data_)
        else:
            await Logger.error_async(f"Unknown compression flag: {repr(flag)}")

        return d

    async def _verify_compression(self, original: pathlib.Path, candidate: pathlib.Path):
        try:
            if not candidate.exists():
                return original

            if candidate.stat().st_size >= original.stat().st_size:
                await Logger.info_async(f"{candidate.name} did not reduce size; keeping original.")
                candidate.unlink(missing_ok=True)
                return original
            
            candidate.replace(original)
            return candidate
        except Exception as e:
            candidate.unlink(missing_ok=True)
            await Logger.error_async(f"Failed validating compressed file: {repr(e)}")
            return original

    async def compress_png(self, input_path: pathlib.Path):
        output_path = input_path.with_name(f"{input_path.stem}_temp{input_path.suffix}")

        def process():
            with Image.open(input_path) as image:
                image.save(output_path, format="PNG", optimize=True, compress_level=9)

        await Logger.info_async('Compressing PNG...')
        try:
            await asyncio.to_thread(process)
            return await self._verify_compression(input_path, output_path)
        except Exception as e:
            output_path.unlink(missing_ok=True)
            await Logger.error_async(f"PNG compression failed: {repr(e)}")
            return input_path

    async def compress_jpeg(self, input_path: pathlib.Path):
        if input_path.suffix.lower() not in ('.jpg', '.jpeg'):
            return input_path

        output_path = input_path.with_name(f"{input_path.stem}_temp{input_path.suffix}")

        def process():
            with Image.open(input_path) as image:
                icc_profile = image.info.get("icc_profile")
                quality = image.info.get("quality", 85)
                save_kwargs = {
                    "format": "JPEG",
                    "optimize": True,
                    "quality": quality,
                    "progressive": True
                }
                if icc_profile:
                    save_kwargs["icc_profile"] = icc_profile

                image.save(output_path, **save_kwargs)

        await Logger.info_async('Compressing JPG...')

        try:
            await asyncio.to_thread(process)
            return await self._verify_compression(input_path, output_path)
        except Exception as e:
            output_path.unlink(missing_ok=True)
            await Logger.error_async(f"JPEG compression failed: {repr(e)}")
            return input_path

    async def compress_video(self, input_path: pathlib.Path):
        if not self.ffmpeg:
            return input_path
        
        info = await self.get_media_data(input_path)
        if not info:
            await Logger.warn_async("Could not probe video; skipping compression.")
            return input_path

        video_streams = [s for s in info.get("streams", []) if s.get("codec_type") == "video"]
        if not video_streams:
            return input_path

        codec_name = video_streams[0].get("codec_name", "").lower()
        output_path = input_path.with_name(f"{input_path.stem}_temp{input_path.suffix}")
        
        ENCODER_MAP = {
            "h264": ("libx264", ["-crf", "23", "-preset", "medium"]),
            "hevc": ("libx265", ["-crf", "28", "-preset", "medium"]),
            "h265": ("libx265", ["-crf", "28", "-preset", "medium"]),
            "vp9": ("libvpx-vp9", ["-crf", "30", "-b:v", "0"]),
            "av1": ("libsvtav1", ["-crf", "32", "-preset", "6"]),
        }

        encoder, encoding_args = ENCODER_MAP.get(codec_name, (None, []))
        if not encoder:
            await Logger.warn_async(f"Codec '{codec_name}' not supported for re-encoding; skipping.")
            return input_path

        args = [
            self.ffmpeg,
            "-y",
            "-i", str(input_path),
            "-map", "0",
            "-map_metadata", "0",
            "-c:v", encoder,
            *encoding_args,
            "-c:a", "copy",
            "-c:s", "copy",
            str(output_path)
        ]

        await Logger.info_async('Compressing Video...')

        code, _, stderr = await self._run_ffmpeg(args)
        if code != 0:
            output_path.unlink(missing_ok=True)
            await Logger.error_async(f"Video compression failed: {stderr.decode('utf-8', errors='replace')}")
            return input_path

        return await self._verify_compression(input_path, output_path)

    async def compress_media(self, file_path: str | pathlib.Path):
        path = pathlib.Path(file_path)
        ext = path.suffix.lower()

        if ext == ".png":
            return await self.compress_png(path)
        elif ext in (".jpg", ".jpeg"):
            return await self.compress_jpeg(path)
        elif ext in (".mp4", ".mkv", ".mov", ".avi"):
            return await self.compress_video(path)
        
        return path

    async def get_media_data(self, file_path: str | pathlib.Path):
        file_path = pathlib.Path(file_path)
        if not file_path.exists() or not self.ffprobe:
            return {}

        args = [
            self.ffprobe,
            "-v", "error",
            "-of", "json",
            "-show_format",
            "-show_streams",
            str(file_path),
        ]

        code, stdout, stderr = await self._run_ffmpeg(args)

        if code != 0:
            await Logger.error_async(f"ffprobe failed: {stderr.decode('utf-8', errors='replace')}")
            return {}

        try:
            return json.loads(stdout.decode("utf-8", errors="replace"))
        except json.JSONDecodeError as e:
            await Logger.error_async(f"Failed parsing ffprobe JSON: {repr(e)}")
            return {}

    async def hash_data(self, data: bytes, prev_hasher=None):
        hasher = prev_hasher if prev_hasher is not None else hashlib.sha256()
        try:
            await asyncio.to_thread(hasher.update, data)
            return hasher.hexdigest()
        except Exception as e:
            await Logger.error_async(f"An error occurred while hashing: {repr(e)}")
            return ""