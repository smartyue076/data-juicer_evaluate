#!/usr/bin/env python3
import os
import shutil
import sys

# =========================
# 配置区
# =========================
base_path = "/home/xuyue/cost_estimate/data-juicer_result/text_pipeline"

# 🔒 白名单：这些 result.jsonl 文件将被跳过（不记录、不删除）
# 使用相对于 base_path 的路径
whitelist_relative = {
    # "after_mapper/result.jsonl",
    # "after_mapper-4/result.jsonl",
}

# 转为绝对路径集合
whitelist_abs = {
    os.path.abspath(os.path.join(base_path, p))
    for p in whitelist_relative
}

# =========================
# 基本检查
# =========================
if not os.path.isdir(base_path):
    print(f"❌ 目录不存在: {base_path}")
    sys.exit(1)

deleted_count = 0
skipped_count = 0
trace_deleted_count = 0
stats_deleted_count = 0

# =========================
# 主逻辑
# =========================
for result_path in os.listdir(base_path):
    result_dir = os.path.join(base_path, result_path)

    # 只处理目录
    if not os.path.isdir(result_dir):
        continue

    for root, dirs, files in os.walk(result_dir):
        # -------------------------
        # 🗑️ 删除 trace 目录
        # -------------------------
        if "trace" in dirs:
            trace_dir = os.path.join(root, "trace")
            try:
                shutil.rmtree(trace_dir)
                trace_deleted_count += 1
                print(f"🧹 已删除 trace 目录: {trace_dir}")
            except OSError as e:
                print(f"⚠️ 删除 trace 失败: {trace_dir} - {e}")

            # 防止 os.walk 继续进入 trace
            dirs.remove("trace")

        # -------------------------
        # 处理文件
        # -------------------------
        for file in files:
            # ===== result.jsonl =====
            if file == "result.jsonl":
                json_path = os.path.abspath(os.path.join(root, file))

                # 白名单检查
                if json_path in whitelist_abs:
                    skipped_count += 1
                    print(f"🔒 跳过（白名单）: {json_path}")
                    continue

                try:
                    size = os.path.getsize(json_path)

                    # 📄 在 jsonl 所在目录创建 result_size.txt
                    size_file = os.path.join(root, "result_size.txt")
                    with open(size_file, "w", encoding="utf-8") as f:
                        f.write(f"{file}: {size} bytes\n")

                    # 🗑️ 删除 jsonl 文件
                    os.remove(json_path)
                    deleted_count += 1
                    print(f"✅ 已记录并删除: {json_path} → 大小已写入 {size_file}")

                except (OSError, IOError) as e:
                    print(f"⚠️ 处理失败: {json_path} - {e}")

            # ===== result_stats.jsonl =====
            elif file == "result_stats.jsonl":
                stats_path = os.path.abspath(os.path.join(root, file))
                try:
                    os.remove(stats_path)
                    stats_deleted_count += 1
                    print(f"🗑️ 已删除: {stats_path}")
                except OSError as e:
                    print(f"⚠️ 删除失败: {stats_path} - {e}")

# =========================
# 汇总输出
# =========================
print("\n✅ 完成！")
print(f"   删除 result.jsonl 数: {deleted_count}")
print(f"   删除 result_stats.jsonl 数: {stats_deleted_count}")
print(f"   删除 trace 目录数: {trace_deleted_count}")
print(f"   跳过（白名单）: {skipped_count}")
print("📄 每个被删除的 result.jsonl 所在目录下已生成 result_size.txt")
