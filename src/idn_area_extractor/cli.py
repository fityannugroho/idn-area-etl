import camelot
import pandas as pd
import typer
from tqdm import tqdm
from pathlib import Path

app = typer.Typer(help="Ekstraksi tabel wired dari PDF dan bersihkan nama wilayah.")

def clean_nama(nama: str) -> str:
    """
    Bersihkan nama wilayah:
    - Hapus angka+\n di awal
    - Hapus \n+angka di akhir
    - Ganti sisa \n jadi spasi
    """
    if not isinstance(nama, str):
        return ""
    nama = (
        nama
        .strip()
        .replace("\r", "")
        .replace("\t", " ")
    )
    nama = pd.Series(nama).str.replace(r"^\d+\n", "", regex=True)
    nama = nama.str.replace(r"\n\d+$", "", regex=True)
    nama = nama.str.replace(r"\n+", " ", regex=True)
    return nama.iloc[0].strip()

@app.command()
def extract(
    pdf_path: Path = typer.Argument(..., exists=True, help="Path ke file PDF"),
):
    typer.echo(f"📄 Membaca file: {pdf_path}")

    # Ekstrak semua tabel menggunakan Camelot (wired table → lattice)
    tables = camelot.read_pdf(str(pdf_path), pages="all", flavor="lattice")
    total_tables = tables.n

    if total_tables == 0:
        typer.echo("❌ Tidak ada tabel terdeteksi di file PDF.")
        raise typer.Exit(code=1)

    typer.echo(f"📊 Jumlah tabel terdeteksi: {total_tables}")

    # Bersihkan dan ekstrak kode + nama
    cleaned_data = []
    for i in tqdm(range(total_tables), desc="🔍 Memproses tabel"):
        df = tables[i].df
        if df.shape[1] < 3:
            continue
        df_clean = df.iloc[2:, [1, 2]].copy()
        df_clean[2] = df_clean[2].apply(clean_nama)
        cleaned_data.append(df_clean)

    if not cleaned_data:
        typer.echo("⚠️ Tidak ada tabel yang valid untuk diolah.")
        raise typer.Exit(code=1)

    df_final = pd.concat(cleaned_data, ignore_index=True)
    df_final.columns = ["Kode", "Nama Wilayah"]

    # Tentukan output file
    output = pdf_path.with_suffix(".csv")

    df_final.to_csv(output, index=False)

    typer.echo(f"✅ Ekstraksi selesai.")
    typer.echo(f"🧾 Jumlah baris hasil konversi: {len(df_final)}")
    typer.echo(f"📁 File output disimpan di: {output.resolve()}")

if __name__ == "__main__":
    app()
