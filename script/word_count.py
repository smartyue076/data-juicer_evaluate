import json
import sys


def count_tokens(text):
    # 按空格划分 token
    return len(text.split())

def analyze_jsonl(file_path):
    total = 0

    max_chars = 0
    min_chars = float("inf")

    max_tokens = 0
    min_tokens = float("inf")

    sum_chars = 0
    sum_tokens = 0

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            text = data.get("text", "")

            char_count = len(text)
            token_count = count_tokens(text)

            total += 1

            max_chars = max(max_chars, char_count)
            min_chars = min(min_chars, char_count)

            max_tokens = max(max_tokens, token_count)
            min_tokens = min(min_tokens, token_count)

            sum_chars += char_count
            sum_tokens += token_count

    print("====== Statistics ======")
    print(f"JSON count: {total}")

    print(f"Min chars: {min_chars}")
    print(f"Max chars: {max_chars}")
    print(f"Avg chars: {sum_chars / total:.2f}")

    print(f"Min tokens: {min_tokens}")
    print(f"Max tokens: {max_tokens}")
    print(f"Avg tokens: {sum_tokens / total:.2f}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 word_count.py file.jsonl")
        sys.exit(1)

    analyze_jsonl(sys.argv[1])