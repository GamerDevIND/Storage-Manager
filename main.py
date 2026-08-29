from file_proccesser import FileProcesser, RAW
from files_io import FileIO
from file_manager import DataBase
import asyncio
import os
from hashlib import sha256
from pprint import pp
from utils import Logger, File, Blob, FileChunk
from providers.local_provider import LocalProvider
from pathlib import Path
# from journal import Journal_manager, Journal

class FileManager:
    class BLOB_VERIED_FAILED_SENTINAL:
        pass
    _BLOB_VERIED_FAILED_SENTINAL = BLOB_VERIED_FAILED_SENTINAL()

    def __init__(self, index_name = 'index.db') -> None:
        self.file_io_manager = FileIO()
        self.file_processer = FileProcesser()
        self.index_manager = DataBase(index_name)
        self.provider = LocalProvider()
        # self.journal_manager = Journal_manager()
        self.gc_task = None
        Logger._get_async_lock(True)

    async def _gc_blob(self):
        while 1:
            try:
                await self.index_manager.garbage_collect(self.provider.delete_blob)
                await asyncio.sleep(60 * 60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                e = repr(e)
                await Logger.error_async(f'GC Task had an error: {e}')
                break

    async def init(self, cdc_min_size: int = 1024 * 256, cdc_avg_size: int = 1024 * 1024, cdc_max_size: int = 1024 * 1024 * 4,):
        await self.index_manager.init()
        await self.provider.init()
        await Logger.info_async('Attempting to use `fastcdc` for chunking...')
        c = FileIO.CDC_Chunker(cdc_min_size, cdc_avg_size, cdc_max_size)
        if not c.installed:
            c = FileIO.FixedChunker()
            await Logger.warn_async('`fastcdc` is not initated properly fallbacking to fixed chunking...')
        else:
            await Logger.info_async('`fastcdc` successfully initiated')

        self.file_io_manager.default_chunker = c
        await self.provider.verify()
        await self.index_manager.verify(self.provider.verify_blob, self.provider.get_blob, 
                                        self.file_processer.decompress_bytes)
        
        self.gc_task = asyncio.create_task(self._gc_blob())

    async def close(self):
        await self.provider.close()
        if self.gc_task:
            try:
                self.gc_task.cancel()
                await self.gc_task
            except asyncio.CancelledError:
                pass
            if self.gc_task.cancelled():
                await Logger.success_async(f'Blob garbage collection task closed successfully.')
        await self.index_manager.close()
        # await self.journal_manager.delete_journal()

    async def index_file(self, file_path:str, chunk_size_KB: int | float | None = 1024 * 10, metadata: dict | None = None, atomic_commit = True):
        metadata = metadata or {}
        file_path = file_path.strip()
        file_hasher = sha256()
        file_name = os.path.basename(file_path)
        # await self.journal_manager.create_journal('Index file')
        # await self.journal_manager.append_step({
        #     'step': "Attempting to compress media."
        # })
        output_path = await self.file_processer.compress_media(file_path)
        # await self.journal_manager.append_step({
        #     'step': 'Compressed media',
        #     'success': True
        # })
        media_compressed = str(output_path) != str(Path(file_path))
        active_path = str(output_path) if media_compressed else file_path
        file_size = os.path.getsize(active_path)
        if media_compressed:
            reader = self.file_io_manager.read(str(output_path), chunk_size_KB)
            compress = False
        else:
            reader = self.file_io_manager.read(file_path, chunk_size_KB)
            compress = True
            
        if active_path.lower().endswith(('.zip', '.rar')):
            await Logger.warn_async('Archives cannot be compressed.')
            compress = False

        idx = 0
        chunks:list[Blob] = []
        async for chunk in reader:
            data = chunk.data
            file_hasher.update(data)
            size = chunk.size
            offset = chunk.offset
            hashed = await self.file_processer.hash_data(data)

            compressed_size = len(data)
            if compress:
                compressed = await self.file_processer.compress_bytes(data)
                compression_type = compressed.compression_type
                data = compressed.data
                compressed_size = len(data)
            else:
                compression_type = RAW

            compressed_hash = await self.file_processer.hash_data(data)
            blob = Blob(blob_hash=hashed, size=size, compressed_size=compressed_size, compression_type=compression_type, ref_count=0, 
                        post_processed_hash=compressed_hash, metadata=metadata, offset=offset, data=data)
            
            await self.provider.put_blob(blob, blob, blob, metadata, self.file_io_manager.write)
            blob.data = None
            chunks.insert(idx, blob)
            await self.index_manager.index_blob(blob)
            idx += 1

        if media_compressed:
            output_path.unlink(missing_ok=True)

        file = File(file_name, file_size, file_hasher.hexdigest(), chunks, file_path, metadata, "LINKING")
        if not atomic_commit:
            await Logger.info_async('Using the normal file linking method')
            await self.index_manager.index_file(file)
            await Logger.info_async(f'Linking blobs to file {file.file_name}')
            total_blobs = len(chunks)
            for i, b in enumerate(chunks):
                await Logger.log_async(f'Linked {i + 1} / {total_blobs} blobs.', 'info', save_to_file=False)
                await self.index_manager.link_file_blob(file.file_hash, b.blob_hash, i, b.offset, round(b.size))
        else:
            await Logger.info_async('Using the file transaction linking method')
            await self.index_manager.index_file_transaction(file, chunks)

        await self.index_manager.exec("UPDATE files SET index_status = 'COMMITTED' WHERE file_hash = ?", [file.file_hash])

        file.index_status = "COMMITTED"

        await Logger.success_async(f'{file.file_path} has been indexed successfully.')

    async def _verify_blob(self, blob: dict | Blob):
        if isinstance(blob, Blob):
            raise NotImplementedError # too lazy to serialise rn

        blob_hash = blob['blob_hash']
        blob_compressed_size = blob['blob_compressed_size']
        blob_compression_type = blob['blob_compression_type']
        blob_post_processed_hash = blob['blob_post_processed_hash']
        blob_chunk_size = blob['blob_chunk_size']
        stored_blob = await self.provider.get_blob(blob_hash)
        if not stored_blob:
            await Logger.error_async(f'No stored blob {blob_hash} found; Aborting...')
            return self._BLOB_VERIED_FAILED_SENTINAL
        
        stored_blob = self.file_processer.CompressedBlob(stored_blob, blob_compression_type)
        if len(stored_blob.data) != blob_compressed_size:
            await Logger.warn_async(f'Blob {blob_hash} stored size ({len(stored_blob.data)} bytes) does NOT match the indexed stored blob size ({blob_compressed_size} bytes);',
                                     'Continuing...')

        blob_hashed = await self.file_processer.hash_data(stored_blob.data)
        if blob_hashed != blob_post_processed_hash:
            await Logger.warn_async(f'Blob {blob_hash} has stored hash of {blob_post_processed_hash} but the stored data hash is {blob_hashed} (NOT MATCHED); Aborting...')
            return self._BLOB_VERIED_FAILED_SENTINAL

        dec = await self.file_processer.decompress_bytes(stored_blob)
        if not dec:
            await Logger.warn_async(f'No bytes found for blob {blob_hash}; Continuing...')
                
        if len(dec) != blob_chunk_size:
            await Logger.warn_async(f'Blob {blob_hash} size ({len(dec)} bytes) does NOT match the indexed blob size ({blob_chunk_size} bytes) (NOT MATCHED); Aborting...')
            return self._BLOB_VERIED_FAILED_SENTINAL
                
        blob_hashed = await self.file_processer.hash_data(dec)
                
        if blob_hashed != blob_hash:
            await Logger.warn_async(f'Blob {blob_hash} has hash of {blob_hash} but the decoded data hash is {blob_hashed} (NOT MATCHED); Aborting...')
            return self._BLOB_VERIED_FAILED_SENTINAL

        return dec

    async def seek_range(self, file_name, file: None | dict = None, starting_range = 0, ending_range = 1024):
        file = file or await self.index_manager.seek_file(file_name=file_name, starting_range=starting_range, ending_range=ending_range)
        if not file:
            await Logger.warn_async("No file provided. Aborting...")
            return

        blobs = file['blobs']
        file_hash = file['file_hash']
        for blob in blobs:
            blob_chunk_idx = blob['blob_chunk_idx']
            blob_offset = blob['blob_offset']
            blob_hash = blob['blob_hash']
            blob_chunk_size = blob['blob_chunk_size']
            blob_end = blob_offset + blob_chunk_size
            if (blob_offset < ending_range) and (blob_end > starting_range):
                blob_data = await self._verify_blob(blob)
                if blob_data == self._BLOB_VERIED_FAILED_SENTINAL or isinstance(blob_data, self.BLOB_VERIED_FAILED_SENTINAL):
                    await Logger.error_async(f'An error occured while seeking {file_name} on blob chunk #{blob_chunk_idx}. Hash: {blob_hash}', 'Aborting...')
                    return

                slice_start = max(0, starting_range - blob_offset)
                slice_end = min(blob_chunk_size, ending_range - blob_offset)
                exact_blob_data = blob_data[slice_start:slice_end]
                yield FileChunk(file_hash=file_hash, chunk_hash=blob_hash, chunk_size=blob_chunk_size, chunk_offset=blob_offset, chunk_idx=blob_chunk_idx, 
                                chunk_data=blob_data, exact_chunk_data=exact_blob_data)

    async def retrieve_file_stream(self, file_name, file: None | dict = None):
        file = file or await self.index_manager.get_file(file_name)
        if not file:
            await Logger.warn_async("No file provided. Aborting...")
            return
        
        file_hash = file['file_hash']
        file_hasher = sha256()
        blobs = file['blobs']
        for blob in blobs:
            blob_chunk_idx = blob['blob_chunk_idx']
            dec = await self._verify_blob(blob)
            if dec == self._BLOB_VERIED_FAILED_SENTINAL or isinstance(dec, self.BLOB_VERIED_FAILED_SENTINAL):
                await Logger.error_async(f'Blob at #{blob_chunk_idx} for {file_name} verification failed. Aborting...')
                return
            
            file_hasher.update(dec)
            yield dec

        if file_hasher.hexdigest() != file_hash:
            await Logger.warn_async(f'{file_name} hash does not match the stored hash!')
            return
        else:
            await Logger.success_async(f'{file_name} retrieved successfully')

    async def save_file_to_path(self, file_name:str, file_save_dir = './'):
        file = await self.index_manager.get_file(file_name)

        if not file:
            await Logger.warn_async("No file provided. Aborting...")
            return
        
        file_hash = file['file_hash']
        file_blob_gen = self.retrieve_file_stream(file_name, file)
        file_path = os.path.join(file_save_dir, file_name)
        await self.file_io_manager.stream_write(file_path, file_blob_gen)
        await Logger.info_async(f'{file_name} written on {file_path}.')
        reader = self.file_io_manager.read(file_path, 1024)
        file_hasher = sha256()
        async for chunk in reader:
            data = chunk.data
            file_hasher.update(data)

        if file_hasher.hexdigest() != file_hash:
            await Logger.error_async(f'File {file_name} hash does not match the index stored hash!')
        else:
            await Logger.success_async(f'{file_name} successfully saved')