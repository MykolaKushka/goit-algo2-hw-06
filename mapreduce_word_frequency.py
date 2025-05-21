import requests
import re
from collections import Counter
from multiprocessing import Pool, cpu_count
import matplotlib.pyplot as plt

# Кількість потоків для паралельної обробки
NUM_PROCESSES = cpu_count()

# --- MapReduce функції ---
def map_words(text_chunk):
    """Map: розбиває текст на слова та підраховує"""
    words = re.findall(r'\b\w+\b', text_chunk.lower())
    return Counter(words)

def reduce_counters(counters):
    """Reduce: об’єднує часткові підрахунки"""
    total = Counter()
    for counter in counters:
        total.update(counter)
    return total

# --- Візуалізація ---
def visualize_top_words(word_counts, top_n=10):
    """Будує горизонтальну гістограму топ-слів"""
    most_common = word_counts.most_common(top_n)
    words, counts = zip(*most_common)

    plt.figure(figsize=(10, 6))
    plt.barh(words, counts, color='skyblue')
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title(f"Top {top_n} Most Frequent Words")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.show()

# --- Основний блок ---
def main():
    url = 'https://www.gutenberg.org/files/1342/1342-0.txt'  # Наприклад: "Pride and Prejudice" by Jane Austen

    # Завантаження тексту
    response = requests.get(url)
    response.raise_for_status()
    text = response.text

    # Розбиття на частини для паралельної обробки
    chunk_size = len(text) // NUM_PROCESSES
    chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

    # Запуск MapReduce з використанням пулу процесів
    with Pool(processes=NUM_PROCESSES) as pool:
        mapped = pool.map(map_words, chunks)
        reduced = reduce_counters(mapped)

    # Побудова графіка
    visualize_top_words(reduced, top_n=10)

if __name__ == "__main__":
    main()
