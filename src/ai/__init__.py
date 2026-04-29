"""OpenAI-based log summarization."""
import logging
import os

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are an Automic Automation Engine log analyst. "
    "Summarize the job execution result in 2-3 sentences. "
    "Clearly state whether it succeeded or failed, and if failed, describe the error and root cause."
)


class OpenAIClient:
    def __init__(self) -> None:
        from openai import OpenAI  # deferred so missing package gives a clear error at use time
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError("OPENAI_API_KEY environment variable is not set")
        self._client = OpenAI(api_key=api_key)

    def summarize(self, combined_log: str) -> str:
        if not combined_log.strip():
            return "No log content available."
        try:
            response = self._client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": f"Summarize this Automic job log:\n\n{combined_log[:8000]}"},
                ],
                max_tokens=200,
                temperature=0,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"OpenAI summarization failed: {e}")
            return f"Summary unavailable: {e}"
