import os
import shutil
import pandas as pd


csv_path = '/Users/tristan/Desktop/FIT5120/QDE/id_name.csv'
video_folder = '/Users/tristan/Desktop/FIT5120/QDE/STU'
output_folder = '/Users/tristan/Desktop/FIT5120/QDE/video_dictionary'


os.makedirs(output_folder, exist_ok=True)


df = pd.read_csv(csv_path)
pairs = []


for col in range(0, len(df.columns), 2):
    name_col = df.columns[col]
    id_col = df.columns[col + 1]
    for name, vid_id in zip(df[name_col], df[id_col]):
        if pd.notna(name) and pd.notna(vid_id):
            vid_id = str(int(vid_id)).zfill(5)
            pairs.append((vid_id, name))


all_files = os.listdir(video_folder)
copied, missing = 0, []

for vid_id, name in pairs:
    match = next((f for f in all_files if f.startswith(vid_id) and f.endswith('.mp4')), None)
    if match:
        src_path = os.path.join(video_folder, match)
        
        safe_name = name.lower().replace(" ", "_").replace("/", "_").replace("\\", "_")
        dst_path = os.path.join(output_folder, f"{safe_name}.mp4")
        shutil.copy(src_path, dst_path)
        copied += 1
    else:
        missing.append(vid_id)


print(f" Copied {copied} videos to {output_folder}")
if missing:
    print("⚠️ Missing videos for the following IDs:")
    print(missing)