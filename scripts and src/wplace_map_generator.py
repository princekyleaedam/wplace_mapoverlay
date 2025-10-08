import json
import glob
import csv
import os
from PIL import Image
import re

# 🔧 Config
json_folder = "sorted"       # Folder containing .jsonl files
output_png = "output_map.png"
legend_txt = "country_color_map.txt"
country_csv = "countryid_to_name.csv"
color_map = {}

# 🎨 Deterministic color generator for countryId
def get_color(country_id):
    if country_id not in color_map:
        color = ((country_id * 97) % 256,
                 (country_id * 57) % 256,
                 (country_id * 31) % 256)
        color_map[country_id] = color
    return color_map[country_id]

# 📘 Load CSV mapping: countryId → countryName
country_name_map = {}
try:
    with open(country_csv, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cid_key = next((k for k in row.keys() if "id" in k.lower()), None)
            name_key = next((k for k in row.keys() if "name" in k.lower()), None)
            if cid_key and name_key:
                try:
                    country_name_map[int(row[cid_key])] = row[name_key]
                except ValueError:
                    continue
except FileNotFoundError:
    print(f"⚠️ Warning: {country_csv} not found. Country names won't be included.")
except Exception as e:
    print(f"⚠️ Failed to load CSV mapping: {e}")

# 📂 Collect .jsonl files that match the tileY pattern
files = glob.glob(f"{json_folder}/tileY-*-uncompressed.jsonl")
if not files:
    raise FileNotFoundError(f"No matching .jsonl files found in {json_folder}")

# 🧮 Extract numeric Y values from filenames
def extract_tile_number(filename):
    match = re.search(r"tileY-(\d+)-uncompressed\.jsonl$", filename)
    return int(match.group(1)) if match else None

# Sort files numerically by the Y value
files = sorted(files, key=lambda f: extract_tile_number(f))

# 📏 Determine image dimensions from the first file
print("Scanning first file for width...")
first_file = files[0]
width = sum(1 for _ in open(first_file, "r", encoding="utf-8"))
height = len(files)
print(f"Detected dimensions → width={width}, height={height}")

# 🧱 Prepare output image
image = Image.new("RGB", (width, height))

# 🚀 Process each file in strict numerical Y order
print("Processing files in tileY-0 to tileY-2048 order...")
for y, filename in enumerate(files, start=1):
    with open(filename, "r", encoding="utf-8") as f:
        for x, line in enumerate(f):
            try:
                data = json.loads(line)
                country_id = data.get("countryId", 0)
                color = get_color(country_id)
                image.putpixel((x, y - 1), color)
            except json.JSONDecodeError:
                continue  # skip malformed JSON lines

    print(f"Processed {y}/{height} files: {os.path.basename(filename)}")

    # Optional: periodically save progress (every 50 files)
    if y % 50 == 0:
        image.save(output_png)

# 💾 Final save
image.save(output_png)
print(f"\n✅ PNG saved as: {output_png}")

# 🗺️ Save legend (countryId → name → RGB)
with open(legend_txt, "w", encoding="utf-8") as f:
    f.write("countryId,countryName,RGB\n")
    for cid, rgb in sorted(color_map.items()):
        cname = country_name_map.get(cid, "Unknown")
        f.write(f"{cid},{cname},{rgb}\n")

print(f"✅ Legend saved as: {legend_txt}")