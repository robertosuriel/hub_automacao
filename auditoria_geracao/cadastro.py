"""Cadastro das usinas geradoras (seção 3 do HANDOFF)."""
from dataclasses import dataclass
from typing import Optional

# Ordem e rótulo dos grupos no relatório
GRUPOS = ["75/110 Riachão", "83/110", "75/110 Itaguaçu", "83/132", "108/132", "100/132", "110/132"]

COR_GRUPO = {  # cores (hex) de cada faixa de grupo, usadas na imagem
    "75/110 Riachão": "E3E3E3", "83/110": "FFF0CC", "75/110 Itaguaçu": "D9EAD3",
    "83/132": "DCE6F2", "108/132": "EAD1E5", "100/132": "EAD1E5", "110/132": "EAD1E5",
}


@dataclass(frozen=True)
class Usina:
    nome: str
    codigo_cliente: str
    nuc: str
    portal: str                   # "iSolarCloud" | "SolisCloud" | "Fronius"
    id_portal: str                # ps nome/ID no portal
    aba_target: Optional[str]     # aba da planilha do Gersio (None = pendente / sem target)
    grupo: str
    aba_target_max: Optional[str] = None   # Riachão: faixa Min–Max
    mes_ref_seguinte: bool = False         # Riachão: usa a fatura do mês seguinte
    obs_target: str = ""


USINAS = [
    Usina("Riachão", "7084200845", "9.752.662.028-11", "Fronius", "60ce51d8-0630-420d-a7ba-c2f54d2a4b91",
          "Riachão_Min", "75/110 Riachão", aba_target_max="Riachão_Max", mes_ref_seguinte=True),
    Usina("Itaguaçu 1", "7087961308", "9.119.409.028-77", "iSolarCloud", "Usina01", "Ita 1_2_3", "83/110"),
    Usina("Itaguaçu 2", "7086422110", "9.087.755.028-49", "iSolarCloud", "Usina02", "Ita 1_2_3", "83/110"),
    Usina("Itaguaçu 3", "7086396470", "9.430.860.028-04", "iSolarCloud", "Usina03", "Ita 1_2_3", "83/110"),
    Usina("Itaguaçu 4", "7089712445", "8.123.318.028-25", "iSolarCloud", "Usina04", "Ita 4_5_6", "75/110 Itaguaçu"),
    Usina("Itaguaçu 5", "7090822300", "9.784.044.028-91", "iSolarCloud", "Usina05", "Ita 4_5_6", "75/110 Itaguaçu"),
    Usina("Itaguaçu 6", "7090898080", "9.450.649.028-25", "iSolarCloud", "Usina06", "Ita 4_5_6", "75/110 Itaguaçu"),
    Usina("Itaguaçu 7", "7098720140", "8.789.620.028-84", "iSolarCloud", "Usina07", "Ita 7_8_9", "75/110 Itaguaçu"),
    Usina("Itaguaçu 8", "7093708452", "7.473.336.028-80", "iSolarCloud", "Usina08", "Ita 7_8_9", "75/110 Itaguaçu"),
    Usina("Itaguaçu 9", "7093114668", "9.130.142.028-82", "iSolarCloud", "Usina09", "Ita 7_8_9", "75/110 Itaguaçu"),
    Usina("Itaguaçu 10", "7093802912", "9.805.966.028-64", "iSolarCloud", "Usina10", "Ita 10", "83/132"),
    Usina("Itaguaçu 11", "7093807787", "7.487.928.028-28", "iSolarCloud", "Usina11", "Ita 11", "75/110 Itaguaçu"),
    Usina("Itaguaçu 12", "7093810311", "7.157.945.028-10", "iSolarCloud", "Usina12", "Ita 12", "83/110"),
    Usina("Itaguaçu 13", "7095207043", "9.474.729.028-02", "iSolarCloud", "Usina13", "Ita 13", "83/132"),
    Usina("Itaguaçu 14", "7095208228", "7.808.050.028-05", "iSolarCloud", "Usina14", "Ita 14_15", "83/110"),
    Usina("Itaguaçu 15", "7095209127", "8.157.393.028-43", "iSolarCloud", "Usina15", "Ita 14_15", "83/110"),
    Usina("Itaguaçu 16", "7099408493", "7.177.027.028-96", "SolisCloud", "1298491919450633675", "Ita 16_17_18", "108/132"),
    Usina("Itaguaçu 17", "7099472370", "7.846.231.028-65", "SolisCloud", "1298491919450704148", "Ita 16_17_18", "108/132"),
    Usina("Itaguaçu 18", "7099477835", "7.847.121.028-70", "SolisCloud", "1298491919450704520", "Ita 16_17_18", "108/132"),
    Usina("Itaguaçu 19", "7099508218", "9.492.368.028-21", "iSolarCloud", "Usina19", "Xique-xique_Max 83kW 132kWp", "83/132"),
    Usina("Itaguaçu 20", "7102983682", "9.895.736.028-60", "iSolarCloud", "Usina20", None, "100/132",
          obs_target="Pendente"),
    Usina("Itaguaçu 21", "7102413746", "9.874.316.028-38", "iSolarCloud", "Usina21", "Xique-xique_Max 100kW 132kWp", "100/132"),
    Usina("Itaguaçu 22", "7101856725", "9.854.858.028-08", "iSolarCloud", "Usina 22", None, "110/132",
          obs_target="Sem target (110/132 não está na planilha do Gersio)"),
]

POR_NOME = {u.nome: u for u in USINAS}
POR_CODIGO = {u.codigo_cliente: u for u in USINAS}
