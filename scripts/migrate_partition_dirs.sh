#!/bin/bash
# 迁移旧的 partition 目录名到新格式

DATA_DIR="${1:-./data}"

echo "🔄 Migrating partition directories in: $DATA_DIR"
echo ""

# 查找所有表目录
for table_dir in "$DATA_DIR"/tables/*; do
    if [ -d "$table_dir" ]; then
        table_name=$(basename "$table_dir")
        echo "📁 Processing table: $table_name"
        
        # 查找所有 partition 目录
        for partition_dir in "$table_dir"/partition-*; do
            if [ -d "$partition_dir" ]; then
                dir_name=$(basename "$partition_dir")
                
                # 提取旧的 partition ID（数字）
                old_id=$(echo "$dir_name" | sed 's/partition-//')
                
                # 检查是否已经是新格式（19位数字）
                if [ ${#old_id} -eq 19 ]; then
                    echo "  ✓ $dir_name (already migrated)"
                    continue
                fi
                
                # 转换为新格式（19位补零）
                new_id=$(printf "%019d" "$old_id" 2>/dev/null)
                
                if [ $? -eq 0 ]; then
                    new_dir_name="partition-$new_id"
                    new_path="$table_dir/$new_dir_name"
                    
                    echo "  🔄 Renaming: $dir_name -> $new_dir_name"
                    mv "$partition_dir" "$new_path"
                    
                    if [ $? -eq 0 ]; then
                        echo "  ✅ Success"
                    else
                        echo "  ❌ Failed to rename"
                    fi
                else
                    echo "  ⚠️  Skipping non-numeric partition: $dir_name"
                fi
            fi
        done
        echo ""
    fi
done

echo "✅ Migration complete!"
