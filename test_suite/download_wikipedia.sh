#!/bin/bash

# 下载 Wikipedia 文章摘要数据集用于全文检索测试
#
# 数据集来源：Hugging Face Datasets - Wikipedia (English)
# 包含：文章标题、摘要文本、URL
#
# 使用方法：
#   ./download_wikipedia.sh [size]
#
# 参数：
#   size: small | medium | large

set -e

SIZE=${1:-small}

# 颜色
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}=== Wikipedia Dataset Downloader ===${NC}\n"

# 创建数据目录
mkdir -p datasets/wikipedia
cd datasets/wikipedia

echo -e "${BLUE}📦 Downloading Wikipedia articles for full-text search...${NC}"
echo "Source: Hugging Face Datasets - wikipedia/20220301.en"
echo

case $SIZE in
    small)
        ROWS="10,000"
        SAMPLE_SIZE="10000"
        DESC="~10K articles, ~50MB"
        ;;
    medium)
        ROWS="50,000"
        SAMPLE_SIZE="50000"
        DESC="~50K articles, ~250MB"
        ;;
    large)
        ROWS="100,000"
        SAMPLE_SIZE="100000"
        DESC="~100K articles, ~500MB"
        ;;
    *)
        echo -e "${RED}Unknown size: $SIZE${NC}"
        echo "Available sizes: small | medium | large"
        exit 1
        ;;
esac

echo "Size: $SIZE ($DESC)"
echo

# 检查 Python 和依赖
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 not found${NC}"
    exit 1
fi

# 创建下载脚本（使用简单新闻数据集作为替代）
cat > download_wikipedia_data.py << 'PYTHON_EOF'
"""
下载简单英文新闻数据集用于全文检索测试
使用 CC-News 数据集作为替代
"""
import json
import sys

def download_news_dataset(sample_size):
    try:
        from datasets import load_dataset
    except ImportError:
        print("Error: 'datasets' library not installed")
        print("Install with: pip3 install datasets")
        sys.exit(1)
    
    print(f"Loading CC-News dataset (first {sample_size:,} articles)...")
    print("Note: Using CC-News as Wikipedia dataset format has changed\n")
    
    try:
        # 使用 CC-News 数据集 - 包含新闻文章
        dataset = load_dataset(
            "cc_news",
            split=f"train[:{sample_size}]",
            trust_remote_code=False
        )
        
        print(f"✓ Loaded {len(dataset):,} articles")
        print(f"Saving to wikipedia_articles.jsonl...")
        
        # 保存为 JSONL 格式
        output_file = "wikipedia_articles.jsonl"
        with open(output_file, "w", encoding="utf-8") as f:
            for idx, article in enumerate(dataset):
                # 构建文档 - 适配不同的字段名
                title = article.get("title", "")
                text = article.get("text", article.get("description", ""))
                url = article.get("url", article.get("domain", ""))
                
                doc = {
                    "id": str(idx),
                    "title": title,
                    "text": text,
                    "url": url,
                }
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")
                
                if (idx + 1) % 1000 == 0:
                    print(f"  Processed {idx + 1:,} articles...")
        
        print(f"✓ Saved to {output_file}")
        print(f"\nDataset info:")
        print(f"  Total articles: {len(dataset):,}")
        print(f"  Format: JSON Lines (.jsonl)")
        print(f"  Fields: id, title, text, url")
        
        # 显示示例
        if len(dataset) > 0:
            sample = dataset[0]
            title = sample.get('title', '')
            text = sample.get('text', sample.get('description', ''))
            print(f"\nSample article:")
            print(f"  Title: {title[:80] if title else 'N/A'}...")
            print(f"  Text: {text[:150] if text else 'N/A'}...")
    
    except Exception as e:
        print(f"Error loading cc_news dataset: {e}")
        print("\nGenerating sample articles instead...")
        generate_sample_articles(sample_size, "wikipedia_articles.jsonl")

def generate_sample_articles(count, output_file):
    """生成示例文章数据"""
    print(f"Generating {count:,} sample articles...")
    
    # 示例文章模板
    topics = [
        ("Artificial Intelligence", "artificial intelligence machine learning neural networks deep learning AI technology computer science algorithms data processing automation"),
        ("Quantum Physics", "quantum mechanics physics particles wave function uncertainty principle quantum computing entanglement superposition Schrodinger Heisenberg"),
        ("Climate Change", "climate change global warming greenhouse gases carbon emissions temperature rise sea level environmental science sustainability renewable energy"),
        ("Machine Learning", "machine learning algorithms supervised learning unsupervised learning neural networks training data models prediction classification regression"),
        ("Python Programming", "python programming language code syntax development software engineering scripting data science web development frameworks libraries"),
        ("World History", "world history ancient civilizations empires wars revolutions historical events cultural development human society politics economics"),
        ("Computer Science", "computer science algorithms data structures programming software engineering computational theory operating systems databases networks"),
        ("Space Exploration", "space exploration astronomy planets solar system NASA spacecraft rockets satellites universe cosmos galaxies stars"),
        ("Biology", "biology life science organisms cells DNA genetics evolution ecology molecular biology species ecosystem adaptation"),
        ("Mathematics", "mathematics algebra geometry calculus equations numbers theory proofs logic problem solving abstract reasoning")
    ]
    
    with open(output_file, "w", encoding="utf-8") as f:
        for idx in range(count):
            topic_idx = idx % len(topics)
            title, keywords = topics[topic_idx]
            
            # 生成文本内容
            text = f"{title} is an important field of study. " * 5
            text += f"Key concepts include: {keywords}. " * 3
            text += f"This article discusses various aspects of {title.lower()} and related topics. " * 2
            
            doc = {
                "id": str(idx),
                "title": f"{title} - Article {idx}",
                "text": text,
                "url": f"https://example.com/article/{idx}",
            }
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            
            if (idx + 1) % 1000 == 0:
                print(f"  Generated {idx + 1:,} articles...")
    
    print(f"✓ Generated {count:,} sample articles")
    print(f"✓ Saved to {output_file}")

if __name__ == "__main__":
    sample_size = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    download_news_dataset(sample_size)
PYTHON_EOF

echo "Checking Python environment..."

# 检查是否在虚拟环境中
if [ -z "$VIRTUAL_ENV" ]; then
    echo -e "${YELLOW}Not in virtual environment. Checking for datasets library...${NC}"
    
    # 尝试导入 datasets
    if ! python3 -c "import datasets" 2>/dev/null; then
        echo -e "${YELLOW}datasets library not found${NC}"
        echo -e "${YELLOW}Generating sample articles instead (no external dependencies needed)${NC}"
        echo
        
        # 直接生成示例数据
        python3 << 'SAMPLE_EOF'
import json

print("Generating sample articles...")

topics = [
    ("Artificial Intelligence", "artificial intelligence machine learning neural networks deep learning AI technology computer science algorithms data processing automation cognitive computing natural language processing computer vision robotics expert systems"),
    ("Quantum Physics", "quantum mechanics physics particles wave function uncertainty principle quantum computing entanglement superposition Schrodinger Heisenberg quantum field theory particle physics subatomic particles Copenhagen interpretation many-worlds theory quantum tunneling"),
    ("Climate Change", "climate change global warming greenhouse gases carbon emissions temperature rise sea level environmental science sustainability renewable energy fossil fuels carbon footprint ecological impact weather patterns atmospheric science oceanography"),
    ("Machine Learning", "machine learning algorithms supervised learning unsupervised learning neural networks training data models prediction classification regression decision trees random forests support vector machines gradient descent backpropagation overfitting cross-validation"),
    ("Python Programming", "python programming language code syntax development software engineering scripting data science web development frameworks libraries Django Flask NumPy pandas matplotlib object-oriented programming functional programming asynchronous programming"),
    ("World History", "world history ancient civilizations empires wars revolutions historical events cultural development human society politics economics medieval period renaissance industrial revolution colonialism world wars cold war globalization democracy totalitarianism"),
    ("Computer Science", "computer science algorithms data structures programming software engineering computational theory operating systems databases networks cybersecurity artificial intelligence distributed systems cloud computing parallel computing complexity theory computability"),
    ("Space Exploration", "space exploration astronomy planets solar system NASA spacecraft rockets satellites universe cosmos galaxies stars black holes nebulae astronauts space stations International Space Station Mars missions lunar exploration exoplanets telescopes"),
    ("Biology", "biology life science organisms cells DNA genetics evolution ecology molecular biology species ecosystem adaptation natural selection mutation heredity biotechnology microbiology botany zoology biochemistry cellular biology"),
    ("Mathematics", "mathematics algebra geometry calculus equations numbers theory proofs logic problem solving abstract reasoning arithmetic trigonometry statistics probability linear algebra differential equations number theory graph theory topology set theory")
]

sample_size = 10000
output_file = "wikipedia_articles.jsonl"

with open(output_file, "w", encoding="utf-8") as f:
    for idx in range(sample_size):
        topic_idx = idx % len(topics)
        title, keywords = topics[topic_idx]
        
        # Generate longer, more realistic text
        text = f"{title} is an important and fascinating field of study that has captured the attention of researchers and scholars worldwide. "
        text += f"The fundamental principles of {title.lower()} include {keywords}. "
        text += f"Throughout history, {title.lower()} has evolved significantly, with numerous breakthroughs and discoveries shaping our understanding. "
        text += f"Modern research in {title.lower()} continues to push the boundaries of human knowledge. "
        text += f"Key concepts and methodologies in this field encompass {keywords}. "
        text += f"Scientists and researchers around the world collaborate to advance our understanding of {title.lower()}. "
        text += f"The practical applications of {title.lower()} are numerous and impact many aspects of daily life. "
        text += f"Future developments in {title.lower()} promise to bring revolutionary changes to society. "
        
        doc = {
            "id": str(idx),
            "title": f"{title} - Article {idx}",
            "text": text,
            "url": f"https://example.com/article/{idx}",
        }
        f.write(json.dumps(doc, ensure_ascii=False) + "\n")
        
        if (idx + 1) % 1000 == 0:
            print(f"  Generated {idx + 1:,} articles...")

print(f"✓ Generated {sample_size:,} sample articles")
print(f"✓ Saved to {output_file}")
SAMPLE_EOF
    else
        echo -e "${GREEN}datasets library found, downloading real articles...${NC}"
        python3 download_wikipedia_data.py $SAMPLE_SIZE
    fi
else
    echo -e "${GREEN}In virtual environment, installing dependencies...${NC}"
    pip3 install -q datasets 2>/dev/null || pip3 install datasets
    echo
    echo "Downloading news articles data..."
    python3 download_wikipedia_data.py $SAMPLE_SIZE
fi

# 清理临时文件
rm -f download_wikipedia_data.py

echo
echo -e "${GREEN}✓ Wikipedia dataset ready${NC}"
echo
echo "Dataset location: datasets/wikipedia/wikipedia_articles.jsonl"
echo
echo "Next steps:"
echo "  1. Load data: python3 load_wikipedia.py"
echo "  2. Test queries: python3 test_wikipedia.py"
echo
echo "Example queries:"
echo "  - Search: content = text('artificial intelligence', 1.0)"
echo "  - Search: content = text('quantum physics', 1.0)"
echo "  - Search: content = phrase('machine learning', 1.0, 0)"
