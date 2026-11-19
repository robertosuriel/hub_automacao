"""Imagem PNG (WhatsApp) do relatório, desenhada com Pillow (sem navegador)."""
import io
import os

from PIL import Image, ImageDraw, ImageFont

from .cadastro import COR_GRUPO, GRUPOS
from .calculo import AMARELO, CINZA, NEUTRO, VERDE, VERMELHO

W = 2340
AZUL, TEXTO, SUAVE = "#1F3A5F", "#1F2D3D", "#6B7A8D"
PILULA = {VERDE: ("#C6EFCE", "#1E6B2F"), AMARELO: ("#FFE699", "#7A5A00"), VERMELHO: ("#F4B6B6", "#8B1A1A"),
          NEUTRO: ("#EEF1F6", "#1F2D3D"), CINZA: (None, SUAVE)}

_REG = ["arial.ttf", "LiberationSans-Regular.ttf", "DejaVuSans.ttf"]
_NEG = ["arialbd.ttf", "LiberationSans-Bold.ttf", "DejaVuSans-Bold.ttf"]
_DIRS = ["C:/Windows/Fonts", "/usr/share/fonts/truetype/liberation", "/usr/share/fonts/truetype/dejavu",
         "/usr/share/fonts/liberation", "/usr/share/fonts/dejavu"]


def _fonte(tam, negrito=False):
    for nome in (_NEG if negrito else _REG):
        for d in _DIRS:
            p = os.path.join(d, nome)
            if os.path.exists(p):
                return ImageFont.truetype(p, tam)
    return ImageFont.load_default()


def _n(v):
    return f"{v:,.0f}".replace(",", ".")


def _pct(v, sinal=False):
    s = f"{v * 100:+.1f}%" if sinal else f"{v * 100:.1f}%"
    return s.replace(".", ",")


def _cor_delta(v):
    return "#2E7D32" if v >= 0 else "#B3261E"


def _quebra(d, texto, fonte, largura):
    linhas, atual = [], ""
    for p in texto.split():
        t = (atual + " " + p).strip()
        if d.textlength(t, font=fonte) <= largura:
            atual = t
        else:
            linhas.append(atual)
            atual = p
    return linhas + [atual]


def gerar_png(linhas, titulo_mes: str, subtitulo: str, rodape_criterios) -> bytes:
    f12, f16, f19, f21, f30, f44 = (_fonte(s) for s in (14, 17, 20, 22, 30, 54))
    b16, b19, b21, b30, b54 = (_fonte(s, True) for s in (17, 20, 22, 32, 56))
    # notas numeradas
    notas, num = [], {}
    for l in linhas:
        txt = [t for t in (l.obs + ([f"{len(l.dias_sem_dado)} dia(s) sem dado no portal"] if l.dias_sem_dado else [])) if t]
        if l.razao is not None and l.razao < 0.90:
            txt.append(f"Coelba registrou só {l.razao:.0%} do inversor: conferir medidor e contestar")
        if l.razao is not None and l.razao > 1.10:
            txt.append("Coelba acima do inversor: provável falha de monitoramento do portal")
        if l.injetado_fatura is None and l.relatorio is not None:
            txt.append("fatura sem injetado; usado o relatório da Coelba")
        if txt:
            num[l.usina.nome] = len(notas) + 1
            notas.append((l.usina.nome, "; ".join(txt)))
    altura = 260 + 60 + len(linhas) * 52 + len(GRUPOS) * 52 + 250 + len(notas) * 44 + 40 + len(rodape_criterios) * 34 + 60
    img = Image.new("RGB", (W, altura), "#F5F6FA")
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, W, 230], fill=AZUL)
    d.text((72, 40), "SOL ONLINE", font=b21, fill="#F2C94C")
    d.text((72, 84), f"Target × Inversor × Coelba por usina — {titulo_mes}", font=b54, fill="white")
    d.text((72, 168), subtitulo, font=f21, fill="#C9D4E5")

    # colunas (x inicial)
    col = dict(usina=72, portal=290, leit=430, dias=640, target=730, inv=980, coe=1230, ixt=1500, cxt=1760, razao=2030)
    y = 270
    d.rectangle([72, y, W - 72, y + 56], fill=AZUL)
    for k, t in [("usina", "Usina"), ("portal", "Portal"), ("leit", "Leituras"), ("dias", "Dias"), ("target", "Target"),
                 ("inv", "Inversor (kWh)"), ("coe", "Coelba (kWh)"), ("ixt", "Inversor × Target"),
                 ("cxt", "Coelba × Target"), ("razao", "Coelba ÷ Inversor")]:
        d.text((col[k] + 12, y + 14), t, font=b19, fill="white")
    y += 56
    n = 0
    for g in GRUPOS:
        do_grupo = [l for l in linhas if l.usina.grupo == g]
        if not do_grupo:
            continue
        d.rectangle([72, y, W - 72, y + 48], fill="#" + COR_GRUPO[g])
        d.text((col["usina"] + 12, y + 12), f"GRUPO {g}", font=b21, fill=AZUL)
        y += 48
        for l in do_grupo:
            d.rectangle([72, y, W - 72, y + 52], fill="white" if n % 2 == 0 else "#EEF1F6")
            d.rectangle([72, y, 78, y + 52], fill="#" + COR_GRUPO[g])
            n += 1
            ty = y + 13
            nome = l.usina.nome
            d.text((col["usina"] + 12, ty), nome, font=f21, fill=TEXTO)
            if nome in num:
                d.text((col["usina"] + 24 + d.textlength(nome, font=f21), ty - 8), str(num[nome]), font=b16, fill="#B3261E")
            d.text((col["portal"] + 12, ty + 2), {"Fronius": "Solar.web"}.get(l.usina.portal, l.usina.portal), font=f19, fill=SUAVE)
            if l.leit_ant and l.leit_atu:
                d.text((col["leit"] + 12, ty), f"{l.leit_ant:%d/%m} → {l.leit_atu:%d/%m}", font=f21, fill=TEXTO)
                d.text((col["dias"] + 12, ty), str(l.dias), font=f21, fill=TEXTO)
            # target
            if l.target is not None:
                tt = _n(l.target) if l.target_max is None else f"{_n(min(l.target, l.target_max))} a {_n(max(l.target, l.target_max))}"
                d.text((col["target"] + 12, ty), tt, font=b21, fill=AZUL)
            else:
                rot = "Indisponível" if l.usina.aba_target else ("Sem target" if l.usina.obs_target.startswith("Sem") else "Pendente")
                d.text((col["target"] + 12, ty), rot, font=f21, fill=SUAVE)
            d.text((col["inv"] + 12, ty), _n(l.inversor) if l.inversor is not None else "Sem dado", font=b21, fill=AZUL)
            if l.coelba is not None:
                d.text((col["coe"] + 12, ty), _n(l.coelba), font=b21, fill=AZUL)
                if l.origem_coelba == "relatório":
                    x = col["coe"] + 24 + d.textlength(_n(l.coelba), font=b21)
                    d.rounded_rectangle([x, ty + 2, x + 96, ty + 28], 6, fill="#8A94A3")
                    d.text((x + 10, ty + 5), "relatório", font=f16, fill="white")
            else:
                d.text((col["coe"] + 12, ty), "Não veio na fatura", font=b21, fill=AZUL)
            # deltas
            for k, v in (("ixt", l.inv_x_target), ("cxt", l.coelba_x_target)):
                if v is None:
                    d.text((col[k] + 12, ty), "—", font=f21, fill=SUAVE)
                else:
                    d.text((col[k] + 12, ty), _pct(v, True), font=b21, fill=_cor_delta(v))
            # pílula
            bg, fg = PILULA[l.cor]
            txt = _pct(l.razao) if l.razao is not None else "n/c"
            if bg:
                tw = d.textlength(txt, font=b21)
                d.rounded_rectangle([col["razao"] + 8, ty - 6, col["razao"] + 40 + tw, ty + 32], 18, fill=bg)
                d.text((col["razao"] + 24, ty), txt, font=b21, fill=fg)
            else:
                d.text((col["razao"] + 24, ty), txt, font=f21, fill=fg)
            y += 52
    # cartões
    y += 36
    verm = [l for l in linhas if l.cor == VERMELHO]
    amar = [l for l in linhas if l.cor == AMARELO]
    verd = [l for l in linhas if l.cor == VERDE]

    def lst(ls):
        return ", ".join(f"{l.usina.nome} ({l.razao:.0%})" for l in ls) or "nenhuma"

    cards = [("#C0504D", "Coelba registrou bem menos que o inversor", f"{lst(verm)}. Prioridade para conferir o medidor."),
             ("#E8B84A", "Coelba acima do inversor", f"{lst(amar)}: o portal costuma perder dias de dado."),
             ("#6AA84F", "Consistentes", f"{len(verd)} usina(s) entre 95% e 105%.")]
    cw = (W - 144 - 2 * 36) // 3
    for i, (cor, t, txt) in enumerate(cards):
        x = 72 + i * (cw + 36)
        d.rounded_rectangle([x, y, x + cw, y + 190], 14, fill="white")
        d.rounded_rectangle([x, y, x + cw, y + 10], 5, fill=cor)
        d.text((x + 24, y + 30), t, font=b21, fill=AZUL)
        for j, ln in enumerate(_quebra(d, txt, f21, cw - 48)[:4]):
            d.text((x + 24, y + 68 + j * 30), ln, font=f21, fill=TEXTO)
    y += 190 + 40
    for i, (nome, txt) in enumerate(notas, start=1):
        d.text((72, y), str(i), font=b16, fill="#B3261E")
        t = f"{nome}: {txt}"
        d.text((96, y), nome + ":", font=b19, fill=TEXTO)
        d.text((96 + d.textlength(nome + ": ", font=b19), y), txt[:230], font=f19, fill=TEXTO)
        y += 44
    y += 16
    for t in rodape_criterios:
        d.text((72, y), "• " + t, font=f16, fill=SUAVE)
        y += 34
    buf = io.BytesIO()
    img.crop((0, 0, W, y + 30)).save(buf, "PNG")
    return buf.getvalue()
