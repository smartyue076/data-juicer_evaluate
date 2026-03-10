import copy
import itertools
import os
import random
import yaml
from datetime import datetime

# =========================
# 1. 输出目录
# =========================

MODE = "random" 
NUM_PIPELINES = 2000  # 仅在 MODE="random" 时有效
pipeline_start_idx = 2000

CONFIG_OUTPUT_PATH = "/home/xuyue/cost_estimate/data-juicer_evaluate/config/text_pipeline"
RESULT_OUTPUT_PATH = "/home/xuyue/cost_estimate/data-juicer_result/text_pipeline"

os.makedirs(CONFIG_OUTPUT_PATH, exist_ok=True)

# =========================
# 2. dataset 配置（支持自定义 jsonl 数量）
# =========================
DATASET_CONFIGS = [
    {
        "type": "local",
        "path": "/home/xuyue/cost_estimate/data-juicer_dataset/RedPajama/c4/c4-train.01010-of-01024.jsonl",
        "format": "jsonl",
    },
    # {
    #     "type": "local",
    #     "path": "/home/xuyue/cost_estimate/data-juicer_dataset/RedPajama/c4/c4-train.01011-of-01024.jsonl",
    #     "format": "jsonl",
    # },
    # {
    #     "type": "local",
    #     "path": "/home/xuyue/cost_estimate/data-juicer_dataset/RedPajama/c4/c4-train.01012-of-01024.jsonl",
    #     "format": "jsonl",
    # },
]

# =========================
# 3. 基础 pipeline 模板
# =========================
BASE_CONFIG = {
    "project_name": "Data-Juicer-recipes-c4",
    "dataset": {
        "configs": DATASET_CONFIGS
    },
    "export_path": "/path/to/your/output.jsonl",
    "np": 50,
    "open_tracer": True,
    "use_cache": False,
    "process": [
        {"clean_email_mapper": None},
        {"clean_links_mapper": None},
        {"fix_unicode_mapper": None},
        {"punctuation_normalization_mapper": None},
        {"whitespace_normalization_mapper": None},

        # 以下算子参数会被 grid search 覆盖
        {"alphanumeric_filter": {}},
        {"average_line_length_filter": {}},
        {"character_repetition_filter": {}},
        {"language_id_score_filter": {}},
        {"maximum_line_length_filter": {}},
        {"perplexity_filter": {}},
        {"special_characters_filter": {}},
        {"words_num_filter": {}},
        {"word_repetition_filter": {}},

        # deduplicator（一般固定）
        {
            "document_simhash_deduplicator": {
                "tokenization": "space",
                "window_size": 6,
                "lowercase": True,
                "ignore_pattern": r"\p{P}",
                "num_blocks": 6,
                "hamming_distance": 4,
            }
        },
    ],
}

PARAM_GRID = {
    # -------------------------
    # alphanumeric_filter
    # -------------------------
    "alphanumeric_filter": {
        # "tokenization": [True, False],
        "tokenization": [False],
        "min_ratio": [0.5,0.6, 0.7, 0.8],
        "max_ratio": [0.8, 0.9],
    },

    # -------------------------
    # average_line_length_filter
    # -------------------------
    "average_line_length_filter": {
        "max_len": [2000, 3000, 4000],
    },

    # -------------------------
    # character_repetition_filter
    # -------------------------
    "character_repetition_filter": {
        "rep_len": [5, 10],
        "max_ratio": [0.2, 0.3],
    },

    # -------------------------
    # language_id_score_filter
    # -------------------------
    "language_id_score_filter": {
        "min_score": [0.5, 0.6, 0.7],
    },

    # -------------------------
    # maximum_line_length_filter
    # -------------------------
    "maximum_line_length_filter": {
        "max_len": [3000, 4000, 5000],
    },

    # -------------------------
    # perplexity_filter
    # -------------------------
    "perplexity_filter": {
        "lang": ["en"],
        "max_ppl": [4000, 5000, 6000],
    },

    # -------------------------
    # special_characters_filter
    # -------------------------
    "special_characters_filter": {
        "max_ratio": [0.3, 0.4],
    },

    # -------------------------
    # words_num_filter
    # -------------------------
    "words_num_filter": {
        # "tokenization": [True, False],
        "tokenization": [False],
        "min_num": [10, 20, 50],
        "max_num": [8000, 10000],
    },

    # -------------------------
    # word_repetition_filter
    # -------------------------
    "word_repetition_filter": {
        "lang": ["en"],
        # "tokenization": [True, False],
        "tokenization": [False],
        "rep_len": [5, 10],
        "max_ratio": [0.2, 0.231],
    },

    # -------------------------
    # document_simhash_deduplicator
    # -------------------------
    "document_simhash_deduplicator": {
        "tokenization": ["space"],
        "window_size": [6],
        "lowercase": [True, False],
        # "lowercase": [True],
        "ignore_pattern": ['\p{P}'],
        "num_blocks": [6],
        "hamming_distance": [4],
    },
}


# =========================
# 5. min / max 约束校验
# =========================
def is_valid_params(params: dict) -> bool:
    """
    确保 min < max
    """
    for key, value in params.items():
        if "min_" in key:
            max_key = key.replace("min_", "max_")
            if max_key in params and value >= params[max_key]:
                return False
    return True

# =========================
# 6. 展开参数网格
# =========================
def expand_grid(grid: dict):
    """
    把 PARAM_GRID 转成每个算子的参数组合
    """
    expanded = {}

    for op_name, op_params in grid.items():
        keys = list(op_params.keys())
        values = list(op_params.values())

        combinations = []
        for combo in itertools.product(*values):
            param_dict = dict(zip(keys, combo))
            if is_valid_params(param_dict):
                combinations.append(param_dict)

        expanded[op_name] = combinations

    return expanded


expanded_params = expand_grid(PARAM_GRID)

# =========================
# 7. 组合不同算子的参数
# =========================

if __name__ == "__main__":
    # op_names = list(expanded_params.keys())
    # op_param_lists = [expanded_params[op] for op in op_names]

    # for combo in itertools.product(*op_param_lists):
    #     cfg = copy.deepcopy(BASE_CONFIG)

    #     # current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    #     # cfg["export_path"] = os.path.join(RESULT_OUTPUT_PATH,f"{pipeline_idx:06d}_{current_time}","result.jsonl")

    #     cfg["export_path"] = os.path.join(RESULT_OUTPUT_PATH,f"{pipeline_start_idx:06d}","result.jsonl")

    #     # 写入参数
    #     for op_name, op_params in zip(op_names, combo):
    #         for proc in cfg["process"]:
    #             if op_name in proc:
    #                 proc[op_name] = op_params

    #     # 输出文件
    #     out_path = os.path.join(CONFIG_OUTPUT_PATH,f"{pipeline_start_idx:06d}.yaml")
    #     with open(out_path, "w") as f:
    #         yaml.safe_dump(cfg, f, sort_keys=False)

    #     pipeline_start_idx += 1
    #     if pipeline_start_idx >= 4000:  # 仅生成 2000 个样例
    #         break

    # print(f"Generated {pipeline_start_idx} pipeline yaml files.")


    # =========================
    # 7. 根据模式筛选 Pipeline
    # =========================
    expanded_params = expand_grid(PARAM_GRID)
    op_names = list(expanded_params.keys())
    op_param_lists = [expanded_params[op] for op in op_names]

    # 生成所有可能的组合 (生成器以节省初步内存)
    all_combinations_iter = itertools.product(*op_param_lists)

    if MODE == "grid":
        selected_combinations = list(all_combinations_iter)
        print(f"模式: [Grid], 将生成全量组合: {len(selected_combinations)} 个")
    elif MODE == "random":
        # 注意：如果全集过大，转为 list 会占内存，但为了 random.sample 必须转 list
        # 或者可以使用更高级的随机采样算法，此处先按常规处理
        all_combinations = list(all_combinations_iter)
        total_count = len(all_combinations)
        num_to_sample = min(NUM_PIPELINES, total_count)
        SEED = 42
        random.seed(SEED)
        selected_combinations = random.sample(all_combinations, num_to_sample)
        print(f"模式: [Random], 从 {total_count} 个组合中随机抽取 {num_to_sample} 个")

    # =========================
    # 8. 循环生成 YAML 文件
    # =========================
    for i, combo in enumerate(selected_combinations):
        current_idx = pipeline_start_idx + i
        cfg = copy.deepcopy(BASE_CONFIG)
        
        # 设置当前 pipeline 的结果输出路径
        pipeline_id_str = f"{current_idx:06d}"
        cfg["export_path"] = os.path.join(RESULT_OUTPUT_PATH, pipeline_id_str, "result.jsonl")

        # 填充参数到 process 列表
        for op_name, op_params in zip(op_names, combo):
            for proc in cfg["process"]:
                if op_name in proc:
                    # 你的代码逻辑：直接覆盖原参数字典
                    proc[op_name].update(op_params)

        # 写入文件
        out_file_name = f"{pipeline_id_str}.yaml"
        out_path = os.path.join(CONFIG_OUTPUT_PATH, out_file_name)
        
        with open(out_path, 'w', encoding='utf-8') as f:
            yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)

    print(f"所有 Pipeline 已生成至: {CONFIG_OUTPUT_PATH}")
