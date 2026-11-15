import hashlib
from pathlib import Path

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


def load_prompt(name: str) -> str:
    """Read a prompt template shipped with the package (llm/prompts/<name>.txt)."""
    return (PROMPTS_DIR / f"{name}.txt").read_text()


def prompt_hash(name: str) -> str:
    """Short content hash of a prompt, recorded in eval reports so a result can be tied to the exact prompt text."""
    return hashlib.sha256((PROMPTS_DIR / f"{name}.txt").read_bytes()).hexdigest()[:12]
