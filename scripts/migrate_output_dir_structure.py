#!/usr/bin/env python3
"""
A script to migrate the output directory structure of downloaded replays, which changed between commits.

Old structure:
./output/replays/{replay_id}_{file_sha256}.{ext} where ./output/replays is the output directory, any user defined path.

New structure:
./output/replays/{sum_0_to_1}/{sum_2_to_3}/{sum_remainder}.{ext}
"""
import argparse
import os
import hashlib

def get_new_filename(old_filename: str) -> (tuple[str, str]):

    basename = os.path.basename(old_filename)
    name_part, ext = os.path.splitext(basename)
    if "_" not in name_part:
        raise ValueError(f"Old filename format invalid: {old_filename}")
    replay_id_str, sha256 = name_part.split("_", 1)
    if len(sha256) != 64:
        raise ValueError(f"SHA256 invalid length in filename: {old_filename}")

    sum_0_to_1 = sha256[0:2]
    sum_2_to_3 = sha256[2:4]
    sum_remainder = sha256[4:]
    new_path = os.path.join(sum_0_to_1, sum_2_to_3, f"{sum_remainder}{ext}")
    return sha256, new_path

def migrate_output_dir(output_dir: str) -> None:
    # find all files in output_dir, assuming flat structure
    for entry in os.scandir(output_dir):
        if entry.is_file():
            old_filepath = entry.path
            try:
                sha256, new_rel_path = get_new_filename(old_filepath)

                # ensure sum matches actual file
                with open(old_filepath, "rb") as f:
                    file_data = f.read()
                    actual_sha256 = hashlib.sha256(file_data).hexdigest()
                    if actual_sha256 != sha256:
                        raise ValueError(f"SHA256 mismatch for file {old_filepath}: expected {sha256}, got {actual_sha256}")

                new_full_path = os.path.join(output_dir, new_rel_path)
                new_dir = os.path.dirname(new_full_path)
                os.makedirs(new_dir, exist_ok=True)
                os.rename(old_filepath, new_full_path)
                print(f"Moved: {old_filepath} -> {new_full_path}")
            except ValueError as ve:
                print(f"Skipping file {old_filepath}: {ve}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Migrate output directory structure of downloaded replays.")
    parser.add_argument("--output-dir", type=str, required=True, help="Path to the output directory containing replays.")
    args = parser.parse_args()

    migrate_output_dir(args.output_dir)
    print("Migration completed.")
