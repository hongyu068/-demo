# 高权限账户/交易日志风险监测与异常识别系统

## 项目概述
本系统用于监测高权限账户的交易行为，通过Benford定律检测和行为聚类分析识别异常交易模式。

## 项目结构
```
risk-monitoring/
├── sql/                    # SQL脚本目录
│   ├── data_ingestion/     # 数据接入脚本
│   ├── data_cleaning/      # 数据清洗脚本
│   └── views/              # 视图定义
├── pyspark/                # PySpark代码目录
│   ├── feature_engineering/ # 特征工程
│   ├── benford_detection/   # Benford定律检测
│   ├── clustering/         # 行为聚类
│   └── validation/         # 模型验证
├── dashboard/              # 可视化仪表盘
│   ├── tableau/           # Tableau文件
│   └── superset/          # Superset配置
├── airflow/               # Airflow DAG配置
├── config/                # 配置文件
├── data/                  # 示例数据
└── reports/               # 报告模板
```

## 快速开始

### 1. 环境准备
```bash
# 安装Python依赖
pip install -r requirements.txt

# 配置Hive连接
cp config/hive_config.example.py config/hive_config.py
# 编辑hive_config.py填入实际连接信息
```

### 2. 数据接入测试
```bash
python sql/data_ingestion/test_connection.py
```

### 3. 运行完整流程
```bash
# 数据清洗
python pyspark/data_cleaning/clean_data.py

# 特征工程
python pyspark/feature_engineering/extract_features.py

# Benford检测
python pyspark/benford_detection/benford_analysis.py

# 行为聚类
python pyspark/clustering/behavior_clustering.py

# 生成报告
python reports/generate_report.py
```

## 阶段完成情况
- [x] 2.1 数据接入 - 完成
- [x] 2.2 数据清洗 - 完成  
- [x] 2.3 特征工程 & Benford检测 - 完成
- [x] 2.4 行为聚类 - 完成
- [x] 2.5 模型评估 - 完成
- [x] 2.6 可视化与报表 - 完成
- [x] 2.7 部署与移交 - 完成

## 使用说明
每个阶段都有独立的可执行脚本，可以单独运行测试。所有代码都包含详细的注释和错误处理。 