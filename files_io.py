import aiofiles
from io import BytesIO
from utils import Logger, File, Blob, call_async
from dataclasses import dataclass
import os
import asyncio
from typing import Iterable, AsyncIterable
from pathlib import Path

_SENTINEL = object()

@dataclass
class Chunk:
    data:bytes
    size:int
    offset:int

class FileIO:
    def __init__(self, default_chunker: None | FixedChunker | CDC_Chunker = None) -> None:
        self.default_chunker = default_chunker or self.FixedChunker()

    class FixedChunker:
        def __init__(self) -> None:
            pass

        async def chunk(self, file, chunk_size = 2048 * 10, stream = True):
            buffer = BytesIO()
            offset = 0
            while True:
                b =  await call_async(file.read(chunk_size))

                if not b:
                    break
            
                if stream:
                    yield Chunk(b, len(b), offset)
                else:
                    buffer.write(b)

                offset += len(b)

            if not stream:
                buffer.seek(0)
                yield Chunk(buffer.read(), buffer.getbuffer().nbytes, 0)

    class CDC_Chunker:
        def __init__(self, min_size: int = 1024 * 256, avg_size: int = 1024 * 1024, max_size: int = 1024 * 1024 * 4,) -> None:
            self.installed = False
            self.min_size = min_size
            self.avg_size = avg_size
            self.max_size = max_size
            try:
                from fastcdc import fastcdc
                from fastcdc.fastcdc_py import Chunk
                self.installed = True
            except ImportError:
                Logger.error_sync("`fastcdc` package not installed; Please install it before usage.")

        async def chunk(self, file, _=None, stream=True):
            if not self.installed:
                await Logger.error_async('The `fastcdc` package is not installed; Aborting')
                return
            
            from fastcdc import fastcdc
            from fastcdc.fastcdc_py import Chunk as CDC_Chunk
            buffer = BytesIO()

            async def async_iter_sync(iterator):
                def next_chunk(iterator):
                    try:
                        return next(iterator)
                    except StopIteration:
                        return _SENTINEL
                    
                while True:
                    try:
                        chunk = await asyncio.to_thread(next_chunk, iterator) # type:ignore

                        if chunk is _SENTINEL:
                            break

                        chunk: CDC_Chunk

                        yield chunk

                    except StopIteration:
                        break
            io_file = file
            file = getattr(file, "_file", file)
            file = getattr(file, "name", file)
            await Logger.log_async(f'fastcdc file: {file}', "info", save_to_file=False)
            iterator: Iterable[CDC_Chunk] = fastcdc(file, self.min_size, self.avg_size, self.max_size)
            cdc_chunks = async_iter_sync(iterator)
            async for cdc_chunk in cdc_chunks:
                cdc_chunk: CDC_Chunk
                
                chunk_data = cdc_chunk.data
                if not chunk_data and cdc_chunk.length > 0:
                    await call_async(io_file.seek(cdc_chunk.offset))
                    chunk_data = await call_async(io_file.read(cdc_chunk.length))

                if stream:
                    yield Chunk(chunk_data, cdc_chunk.length, cdc_chunk.offset)
                else:
                    buffer.write(chunk_data)

            if not stream:
                buffer.seek(0)
                yield Chunk(buffer.read(), buffer.getbuffer().nbytes, 0)

    async def read(self, file_path:str | File, chunk_size_KB:int | float | None = 1024 * 10, stream:bool = True, 
                   chunker: FixedChunker | None | CDC_Chunker = None):
        chunker = chunker or self.default_chunker
        chunk_size_KB = chunk_size_KB or 0
        chunk_size = chunk_size_KB * 1024
        chunk_size = round(chunk_size)
        if isinstance(file_path, File):
            if not (file_path.file_path and file_path.file_path.strip()):
                await Logger.error_async(f'No file path provided for "{file_path.file_name}"; Aborting...')
                return
            
            file_path = file_path.file_path
        file_path = str(file_path)
        await Logger.info_async(f'Attempting to read {file_path}...')
        try:
            async with aiofiles.open(file_path, 'rb') as f:
                f.name
                async for chunk in chunker.chunk(f, chunk_size, stream):
                    yield chunk

        except FileNotFoundError:
            await Logger.error_async(f'{file_path} not found, aborting')
            return 
        except OSError as e:
            await Logger.error_async(f'OS error while reading "{file_path}": {repr(e)}; aborting...')
            return 
        except Exception as e:
            await Logger.error_async(f'An error occured while reading "{file_path}": {repr(e)}; aborting...')
            return

    async def write(self, data:bytes | Blob, file_path:str):
        if isinstance(data, Blob):
            data = data.data or bytes()

        if not data:
            await Logger.warn_async(f'No data provided for "{file_path}"; continuing...')

        file_path = str(file_path)

        try:
            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(data)
                await f.flush()
            await Logger.success_async(f'Data successfully saved as "{file_path}"')
        except Exception as e:
            await Logger.error_async(f'An error occured while writing "{file_path}": {repr(e)}')

    async def stream_write(self, file_path, data_gen:Iterable | AsyncIterable | None):
        file_path = str(file_path)
        if not data_gen:
            await Logger.warn_async(f'No data provided for "{file_path}"; aborting')
            return

        await Logger.info_async(f'Streaming writing "{file_path}"')
        try:
            async with aiofiles.open(file_path, 'wb') as f:
                if isinstance(data_gen, Iterable) and not isinstance(data_gen, (bytes, bytearray, memoryview)):
                    for data in data_gen:
                        if isinstance(data, Blob):
                            data = data.data or bytes()
                        if not data:
                            await Logger.warn_async(f'No data provided for "{file_path}"; continuing...')

                        await f.write(data)
                        
                elif isinstance(data_gen, AsyncIterable):
                    async for data in data_gen:
                        if isinstance(data, Blob):
                            data = data.data or bytes()
                        if not data:
                            await Logger.warn_async(f'No data provided for "{file_path}"; continuing...')
                    
                        await f.write(data)
                else:
                    await Logger.error_async(f'Generator (Type:{type(data_gen).__name__}) provided for blob consumption must a iterable or async iterable; Aborting...')

                await f.flush()

            if os.path.getsize(file_path) == 0:
                await Logger.warn_async(f'{file_path} has no bytes. Removing...')
                Path(file_path).unlink(True)
                
            await Logger.success_async(f'Data successfully saved as "{file_path}"')
        except Exception as e:
            await Logger.error_async(f'An error occured while writing "{file_path}": {repr(e)}')