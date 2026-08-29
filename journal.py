import aiofiles
import json
from dataclasses import dataclass, field, is_dataclass, asdict
import os
import re
import asyncio
from utils import Logger
from pathlib import Path
from datetime import datetime

@dataclass
class Journal:
    CREATED = 'CREATED'
    main_intent: str
    status:str
    steps:list[dict] = field(default_factory=list)
    created_on: str = field(default_factory=lambda: datetime.now().strftime("%H:%M:%S - %d / %m / %Y"))
    modified_on: str = field(default_factory=lambda: datetime.now().strftime("%H:%M:%S - %d / %m / %Y"))
    metadata:dict = field(default_factory=dict)

    def add_step(self, step: dict):
        self.steps.append(step)
        self.update_modified()

    def update_modified(self):
        self.modified_on = datetime.now().strftime("%H:%M:%S - %d / %m / %Y")

    def set_status(self, status:str):
        self.status = status
        self.update_modified()

    def get_steps(self):
        return self.steps

    def get_status(self):
        return self.status

    @classmethod
    def from_dict(cls, data:dict):
        intent = data.get('main_intent', data.get('intent', data.get('reason')))
        if not intent:
            raise KeyError("Missing required intent field ('main_intent', 'intent', or 'reason') in journal payload.")
        
        status = data['status']
        steps = data.get('steps', data.get('journal_steps', data.get('journey'))) or list()
        created = data.get('created', data.get('created_on')) or datetime.now().strftime("%H:%M:%S - %d / %m / %Y")
        modified = data.get('modified', data.get('modified_on')) or datetime.now().strftime("%H:%M:%S - %d / %m / %Y")
        metadata = data.get('metadata', data.get('extra_data')) or dict()

        return cls(intent, status, steps, created_on=created, modified_on=modified, metadata=metadata)

    def __post_init__(self):
        if not isinstance(self.main_intent, str) or not self.main_intent.strip():
            raise ValueError("main_intent must be a non-empty string.")
        if not isinstance(self.steps, list):
            raise TypeError("steps must be a list.")
        
        self.status = self.status.strip() or self.CREATED
    
class Journal_manager:
    def __init__(self, journal_dir:str = './') -> None:
        self.journal_dir = Path(journal_dir)
        self.journal_dir.mkdir(parents=True, exist_ok=True)
        self.main_journal = self.journal_dir / 'journal.json'
        self.temp_file = self.journal_dir / 'journal_json.tmp'

    async def get_temp(self):
        try:
            exists = await asyncio.to_thread(os.path.exists, self.temp_file)
            if not exists:
                await Logger.warn_async(f'{self.temp_file} not found in {self.journal_dir}')
                return None

            async with aiofiles.open(self.temp_file, 'r') as f:
                string = await f.read()

            string = string.strip()

            json_block_match = re.search(r'([\{\[].*[\}\]])', string, re.DOTALL)
            if json_block_match:
                string = str(json_block_match.group(1))

            try:
                js: dict = json.loads(string)
                return js
            except json.JSONDecodeError:
                pass

            string = re.sub(r'\bTrue\b', 'true', string)
            string = re.sub(r'\bFalse\b', 'false', string)
            string = re.sub(r"(?<!\\)'", '"', string)
            string = re.sub(r'(?<={|,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'"\1":', string)
            string = re.sub(r'(?<=\{\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'"\1":', string)
            string = re.sub(r',\s*\}', '}', string)
            string = re.sub(r',\s*\]', ']', string)
            js: dict = json.loads(string)
            return js
        except Exception as e:
            e = repr(e)
            await Logger.error_async(f"An error occured while attempting to recover {self.temp_file}: {e}")
            return None

    async def create_journal(self, intent:str):
        j = Journal(intent, Journal.CREATED)
        w = await self.write_journal(j)
        if not w:
            return None
        return j

    async def delete_journal(self):
        await Logger.info_async(f'Deleting the main and temporary journals...')
        await asyncio.to_thread(lambda: self.temp_file.unlink(missing_ok=True))
        await asyncio.to_thread(lambda: self.main_journal.unlink(missing_ok=True))

    async def update_status(self, new_status: str):
        journal = await self.get_journal()
        if not journal:
            return False
        journal.set_status(new_status)
        journal.update_modified()
        return await self.write_journal(journal)

    async def append_step(self, step: dict):
        journal = await self.get_journal()
        if not journal:
            return False
        journal.add_step(step)
        return await self.write_journal(journal)

    async def get_journal(self):
        try:
            async with aiofiles.open(self.main_journal, 'r') as f:
                r = await f.read()
                js: dict = json.loads(r)
                return Journal.from_dict(js)
        except Exception as e:
            e = repr(e)
            await Logger.error_async(f"An error occured while attempting to read {self.main_journal}: {e}")
            return None

    async def write_journal(self, data:dict | Journal):
        if is_dataclass(data):
            data = asdict(data)
        elif not isinstance(data, dict):
            try:
                data = dict(data)
            except Exception as e:
                e = repr(e)
                await Logger.error_async(f'An error occured while attempting to serailze the object: {repr(data)} of type: {type(data).__name__}: {e}')
                return False

        try:
            serialized = json.dumps(data, indent=2, default=str)
        except Exception as e:
            await Logger.error_async(f'Failed to serialize payload to JSON: {repr(e)}')
            return False

        try:
            async with aiofiles.open(self.temp_file, 'w', encoding='utf-8') as f:
                await f.write(serialized)
                await f.flush()
                await asyncio.to_thread(os.fsync, f.fileno())
        except Exception as e:
            await Logger.error_async(f'Error writing temporary journal: {repr(e)}')
            return False

        try:
            await asyncio.to_thread(self.temp_file.replace, self.main_journal)
            return True
        except Exception as e:
            await Logger.error_async(f"Atomic swap failed for {self.main_journal}: {repr(e)}")
            return False