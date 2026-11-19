"""PDF -> texto preservando o layout (pdftotext -layout; fallback pypdfium2)."""
import shutil
import subprocess


def pdf_para_texto(dados: bytes) -> str:
    if shutil.which("pdftotext"):
        r = subprocess.run(["pdftotext", "-layout", "-", "-"], input=dados, capture_output=True, timeout=60)
        if r.returncode == 0 and r.stdout:
            return r.stdout.decode("utf-8", errors="replace")
    import pypdfium2 as pdfium  # fallback: o layout pode sair diferente do pdftotext
    pdf = pdfium.PdfDocument(dados)
    return "\n".join(p.get_textpage().get_text_range() for p in pdf)
