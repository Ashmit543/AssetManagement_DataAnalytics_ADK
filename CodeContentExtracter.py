import os
import sys
from pathlib import Path


def is_code_file(file_path):
    """Check if a file is a code file based on its extension."""
    code_extensions = {
        '.py', '.js', '.ts', '.jsx', '.tsx', '.java', '.cpp', '.c', '.h', '.hpp',
        '.cs', '.php', '.rb', '.go', '.rs', '.swift', '.kt', '.scala', '.sh',
        '.bash', '.ps1', '.sql', '.html', '.css', '.scss', '.sass', '.less',
        '.xml', '.json', '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf',
        '.r', '.m', '.pl', '.lua', '.dart', '.vb', '.f90', '.f95', '.asm',
        '.s', '.hs', '.ml', '.fs', '.clj', '.lisp', '.scheme', '.elm', '.ex',
        '.exs', '.erl', '.jl', '.nim', '.crystal', '.d', '.zig', '.v'
    }
    return file_path.suffix.lower() in code_extensions


def parse_code_files(root_directory, output_file):
    """Parse all code files in directory and write to output file."""
    root_path = Path(root_directory)

    if not root_path.exists():
        print(f"Error: Directory '{root_directory}' does not exist.")
        return

    if not root_path.is_dir():
        print(f"Error: '{root_directory}' is not a directory.")
        return

    code_files = []

    # Walk through all files recursively
    for file_path in root_path.rglob('*'):
        if file_path.is_file() and is_code_file(file_path):
            code_files.append(file_path)

    # Sort files for consistent ordering
    code_files.sort()

    if not code_files:
        print("No code files found in the directory.")
        return

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            for i, file_path in enumerate(code_files):
                # Write file address and name
                f.write(f"Code File address and name:\n{file_path}\n\n")
                f.write("Code file content:\n")

                try:
                    # Read and write file content
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as code_file:
                        content = code_file.read()
                        f.write(content)
                except Exception as e:
                    f.write(f"Error reading file: {e}\n")

                # Add separator between files (except for the last file)
                if i < len(code_files) - 1:
                    f.write("\n\n" + "=" * 80 + "\n\n")

        print(f"Successfully parsed {len(code_files)} code files.")
        print(f"Output written to: {output_file}")

    except Exception as e:
        print(f"Error writing to output file: {e}")


def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) < 2:
        root_dir = input("Enter the root directory path: ").strip()
    else:
        root_dir = sys.argv[1]

    if len(sys.argv) < 3:
        output_file = input("Enter output file name (default: 'parsed_code.txt'): ").strip()
        if not output_file:
            output_file = "parsed_code.txt"
    else:
        output_file = sys.argv[2]

    # Convert to absolute path if relative path is provided
    root_dir = os.path.abspath(root_dir)

    print(f"Parsing code files from: {root_dir}")
    print(f"Output file: {output_file}")

    parse_code_files(root_dir, output_file)


if __name__ == "__main__":
    main()