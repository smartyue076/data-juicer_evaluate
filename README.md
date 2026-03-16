# Data-Juicer Evaluate

## 项目介绍

该项目旨在生成并运行DataJuicer的预处理任务。项目包含pipeline配置，运行脚本等。

## 项目结构

config

- configs_v1.4.3：*DataJuicer v1.4.3*版本提供的参考pipeline。DataJuicer线上版本已移除该部分，故在此备份

- text_pipeline：生成的文本预处理pipeline

env

- build_image.sh：创建包含DataJuicer环境依赖的docker container脚本

script

- run.sh：批量运行pipeline脚本
- word_count.py：批量统计文本数据集的字符数与token数
- create_pipeline_sample.py：以`config/configs_v1.4.3/data_juicer_recipesredpajama-c4-refine.yaml`为模板批量创建pipeline

## 如何运行

### 1. 创建容器

```shell
cd env;
bash build_image.sh;
```

### 2. 进入容器

```shell
docker exec -it ${container_name} bash
```

### 3. 批量运行pipeline

```shell
cd script;
bash run.sh 1 100; # 运行config/text_pipeline的第1到100个pipeline
```

### 服务器文件位置

目前数据集，运行结果，模型都保存在172.23.166.104服务器/data目录下。

- data-juicer_dataset：数据集
- data-juicer_models：算子需要的模型
- data-juicer_result：运行结果

另外，101与104服务器下的xy_dj_test容器包含了运行所需要的环境，可以先在这上面做实验。





