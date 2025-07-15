# 风险监测系统交接文档

## 项目概述

本项目是一个高权限账户/交易日志风险监测与异常识别系统，通过Benford定律检测和行为聚类分析识别异常交易模式。

## 系统架构

### 技术栈
- **数据引擎**: Hive + Spark on YARN (PySpark 2.x/3.x)
- **开发IDE**: Cursor (支持SQL & Python混编)
- **Python依赖**: pandas, pyhive, pyspark, scikit-learn, scipy, seaborn/matplotlib, sqlalchemy
- **可视化**: Tableau Desktop / Apache Superset
- **调度**: Apache Airflow

### 目录结构
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

## 部署说明

### 1. 环境准备
```bash
# 安装Python依赖
pip install -r requirements.txt

# 配置Hive连接
cp config/hive_config.example.py config/hive_config.py
# 编辑hive_config.py填入实际连接信息
```

### 2. 快速部署
```bash
# 运行部署脚本
chmod +x deploy.sh
./deploy.sh
```

### 3. 手动部署步骤
```bash
# 1. 测试连接
python sql/data_ingestion/test_connection.py

# 2. 数据清洗
python pyspark/data_cleaning/clean_data.py

# 3. 特征工程
python pyspark/feature_engineering/extract_features.py

# 4. Benford检测
python pyspark/benford_detection/benford_analysis.py

# 5. 行为聚类
python pyspark/clustering/behavior_clustering.py

# 6. 模型验证
python pyspark/validation/model_validation.py

# 7. 创建仪表盘
python dashboard/create_dashboard.py

# 8. 生成报告
python reports/generate_report.py
```

## 配置说明

### Hive配置 (config/hive_config.py)
```python
HIVE_CONFIG = {
    'host': 'your-hive-server.com',
    'port': 10000,
    'username': 'your_username',
    'password': 'your_password',
    'database': 'default',
    'auth': 'CUSTOM',
    'configuration': {
        'hive.server2.proxy.user': 'your_proxy_user'
    }
}
```

### 风险阈值配置
```python
RISK_THRESHOLDS = {
    'benford_chi2_threshold': 15.0,
    'benford_mad_threshold': 0.015,
    'anomaly_score_threshold': 0.8,
    'night_operation_threshold': 0.3,
    'high_amount_threshold': 1000000
}
```

## 监控和维护

### 1. 日常监控
- 检查Airflow DAG执行状态
- 监控数据质量指标
- 查看异常用户清单
- 验证模型性能指标

### 2. 定期维护
- 每周更新风险阈值
- 每月重新训练模型
- 每季度评估系统性能
- 每年更新技术栈

### 3. 故障处理
- 检查日志文件: logs/
- 验证数据源连接
- 重启失败的Airflow任务
- 联系技术支持

## 输出文件说明

### 数据文件 (data/)
- `risk_fact.csv`: 风险分析宽表
- `featured_data.csv`: 特征工程数据
- `benford_analysis.csv`: Benford分析结果
- `clustering_results.csv`: 聚类结果
- `anomaly_users.csv`: 异常用户清单

### 仪表盘文件 (dashboard/)
- `benford_heatmap_data.csv`: Benford热力图数据
- `cluster_scatter_data.csv`: 聚类散点图数据
- `anomaly_watchlist.csv`: 异常监控列表
- `interactive_dashboard.html`: 交互式仪表盘
- `tableau_config.json`: Tableau配置
- `superset_config.json`: Superset配置

### 报告文件 (reports/)
- `risk_monitoring_report.docx`: Word格式报告
- `risk_monitoring_report.md`: Markdown格式报告
- `risk_monitoring_report.json`: JSON格式报告

## 联系信息

- **项目负责人**: [姓名]
- **技术支持**: [邮箱]
- **运维团队**: [联系方式]

## 更新记录

| 日期 | 版本 | 更新内容 | 负责人 |
|------|------|----------|--------|
| 2025-07-15 | 1.0.0 | 初始版本 | [姓名] |

---
*本文档由风险监测系统自动生成，最后更新时间: 2025年07月15日*
