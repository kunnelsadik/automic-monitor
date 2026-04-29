"""OpenAI client for Automic log information extraction."""
import logging

from openai import OpenAI

from src.config import OpenAIConfig

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are an expert at analyzing Automic job execution logs.
Extract all meaningful information and return ONLY a valid JSON object with these fields:

{
  "files_processed": ["list of filenames found in the logs"],
  "input_locations": ["source file paths or directories"],
  "output_locations": ["destination file paths or directories"],
  "connections_used": ["server names, FTP connections, or database connections"],
  "key_variables": {"variable_name": "value"},
  "file_sizes": ["any file size information mentioned"],
  "runtime_info": "execution time or performance details if present",
  "errors": ["error messages or warnings if any"],
  "notes": "any other important observations about the job"
}

Rules:
- Use empty lists [] or null for fields with no data.
- Include ALL file paths, even partial ones.
- Capture every variable assignment found.
- Return only the JSON object with no markdown or explanation."""


class OpenAIClient:
    def __init__(self, config: OpenAIConfig) -> None:
        self.config = config
        self._client = OpenAI(api_key=config.api_key.get_secret_value())

    def summarize_logs(self, combined_log: str) -> str:
        logger.info("Calling OpenAI for log extraction")
        response = self._client.chat.completions.create(
            model=self.config.model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": combined_log[:15000]},
            ],
            temperature=0,
        )
        return response.choices[0].message.content.strip()
