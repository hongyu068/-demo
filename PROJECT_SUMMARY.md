# 高权限账户风险监测系统 - 项目总结

## 项目完成情况

✅ **所有7个阶段已全部完成**

| 阶段 | 状态 | 完成时间 | 主要交付物 |
|------|------|----------|------------|
| 2.1 数据接入 | ✅ 完成 | 2024-01-01 | Hive连接测试脚本、示例数据 |
| 2.2 数据清洗 | ✅ 完成 | 2024-01-01 | 数据清洗脚本、风险分析宽表 |
| 2.3 特征工程 & Benford检测 | ✅ 完成 | 2024-01-01 | 特征提取脚本、Benford分析脚本 |
| 2.4 行为聚类 | ✅ 完成 | 2024-01-01 | K-Means/DBSCAN聚类脚本 |
| 2.5 模型评估 | ✅ 完成 | 2024-01-01 | 交叉验证、异常检测评估 |
| 2.6 可视化与报表 | ✅ 完成 | 2024-01-01 | Tableau/Superset配置、交互式仪表盘 |
| 2.7 部署与移交 | ✅ 完成 | 2024-01-01 | Airflow DAG、部署脚本、文档 |

## 系统架构

### 技术栈
- **数据引擎**: Hive + Spark on YARN (PySpark 2.x/3.x)
- **开发IDE**: Cursor (支持SQL & Python混编)
- **Python依赖**: pandas, pyhive, pyspark, scikit-learn, scipy, seaborn/matplotlib, sqlalchemy
- **可视化**: Tableau Desktop / Apache Superset / Plotly
- **调度**: Apache Airflow
- **报告**: Word/Markdown/JSON格式

### 核心功能
1. **Benford定律检测**: 分析交易金额首位数字分布，识别异常数字模式
2. **行为聚类分析**: 使用K-Means和DBSCAN对用户行为进行聚类
3. **异常评分计算**: 综合多个风险因子计算用户异常评分
4. **可视化仪表盘**: 提供交互式图表和监控界面
5. **自动化报告**: 生成多种格式的风险监测报告

## 文件结构

```
risk-monitoring/
├── README.md                           # 项目说明
├── requirements.txt                    # Python依赖
├── QUICK_START.md                      # 快速开始指南
├── HANDOVER_DOCUMENTATION.md           # 交接文档
├── PROJECT_SUMMARY.md                  # 项目总结
├── run_full_pipeline.py                # 一键运行脚本
├── run_deployment.py                   # 部署脚本
├── deploy.sh                           # Linux部署脚本
├── config/
│   └── hive_config.example.py         # Hive配置示例
├── sql/
│   └── data_ingestion/
│       └── test_connection.py         # 数据接入测试
├── pyspark/
│   ├── data_cleaning/
│   │   └── clean_data.py              # 数据清洗
│   ├── feature_engineering/
│   │   └── extract_features.py        # 特征工程
│   ├── benford_detection/
│   │   └── benford_analysis.py        # Benford检测
│   ├── clustering/
│   │   └── behavior_clustering.py     # 行为聚类
│   └── validation/
│       └── model_validation.py        # 模型验证
├── dashboard/
│   └── create_dashboard.py            # 仪表盘创建
├── reports/
│   └── generate_report.py             # 报告生成
├── airflow/
│   └── risk_monitoring_dag.py         # Airflow DAG
├── data/                              # 数据文件目录
├── logs/                              # 日志目录
└── reports/                           # 报告输出目录
```

## 核心算法

### 1. Benford定律检测
- **χ²统计量**: 衡量实际分布与期望分布的差异
- **MAD统计量**: 平均绝对偏差
- **风险评分**: 基于阈值的综合评分

### 2. 行为聚类
- **K-Means聚类**: 基于用户行为特征进行分组
- **DBSCAN聚类**: 识别噪声点和异常用户
- **异常评分**: 综合多个风险因子的加权评分

### 3. 特征工程
- **时间特征**: 夜间操作比例、周末操作比例
- **行为特征**: 交易频率、IP多样性、金额特征
- **风险特征**: 高金额标识、异常时间标识

## 使用方法

### 快速开始
```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置连接
cp config/hive_config.example.py config/hive_config.py
# 编辑配置文件

# 3. 一键运行
python run_full_pipeline.py
```

### 分步执行
```bash
# 数据接入测试
python sql/data_ingestion/test_connection.py

# 数据清洗
python pyspark/data_cleaning/clean_data.py

# 特征工程
python pyspark/feature_engineering/extract_features.py

# Benford检测
python pyspark/benford_detection/benford_analysis.py

# 行为聚类
python pyspark/clustering/behavior_clustering.py

# 模型验证
python pyspark/validation/model_validation.py

# 创建仪表盘
python dashboard/create_dashboard.py

# 生成报告
python reports/generate_report.py
```

## 输出文件

### 数据文件
- `data/risk_fact.csv`: 风险分析宽表
- `data/featured_data.csv`: 特征工程数据
- `data/benford_analysis.csv`: Benford分析结果
- `data/clustering_results.csv`: 聚类结果
- `data/anomaly_users.csv`: 异常用户清单

### 可视化文件
- `dashboard/interactive_dashboard.html`: 交互式仪表盘
- `dashboard/benford_heatmap_data.csv`: Benford热力图数据
- `dashboard/cluster_scatter_data.csv`: 聚类散点图数据
- `dashboard/anomaly_watchlist.csv`: 异常监控列表
- `dashboard/tableau_config.json`: Tableau配置
- `dashboard/superset_config.json`: Superset配置

### 报告文件
- `reports/risk_monitoring_report.docx`: Word格式报告
- `reports/risk_monitoring_report.md`: Markdown格式报告
- `reports/risk_monitoring_report.json`: JSON格式报告

## 配置说明

### 风险阈值配置
```python
RISK_THRESHOLDS = {
    'benford_chi2_threshold': 15.0,      # Benford χ²阈值
    'benford_mad_threshold': 0.015,      # Benford MAD阈值
    'anomaly_score_threshold': 0.8,      # 异常评分阈值
    'night_operation_threshold': 0.3,    # 夜间操作比例阈值
    'high_amount_threshold': 1000000     # 高金额阈值
}
```

### Hive连接配置
```python
HIVE_CONFIG = {
    'host': 'your-hive-server.com',
    'port': 10000,
    'username': 'your_username',
    'password': 'your_password',
    'database': 'default',
    'auth': 'CUSTOM'
}
```

## 监控和维护

### 日常监控
- 检查Airflow DAG执行状态
- 监控数据质量指标
- 查看异常用户清单
- 验证模型性能指标

### 定期维护
- 每周更新风险阈值
- 每月重新训练模型
- 每季度评估系统性能
- 每年更新技术栈

## 扩展性

### 添加新的检测规则
在 `pyspark/clustering/behavior_clustering.py` 的 `identify_anomalies` 函数中添加新规则。

### 自定义报告格式
修改 `reports/generate_report.py` 中的报告生成函数。

### 集成新的数据源
在 `sql/data_ingestion/` 目录下添加新的数据接入脚本。

## 项目亮点

1. **完整的端到端流程**: 从数据接入到报告生成的完整链路
2. **多种算法结合**: Benford定律 + 机器学习聚类
3. **丰富的可视化**: 支持Tableau、Superset、Plotly多种可视化工具
4. **自动化程度高**: 支持Airflow调度，一键运行
5. **文档完善**: 提供详细的使用说明和交接文档
6. **容错性强**: 支持示例数据测试，不依赖真实Hive环境
7. **可扩展性好**: 模块化设计，易于添加新功能

## 下一步建议

1. **生产环境部署**: 配置真实的Hive连接和Airflow环境
2. **性能优化**: 针对大数据量进行性能调优
3. **模型优化**: 基于实际数据调整模型参数
4. **监控集成**: 集成到现有的监控系统
5. **告警机制**: 添加实时告警功能
6. **用户界面**: 开发Web管理界面

---

**项目状态**: ✅ 已完成  
**最后更新**: 2024年1月1日  
**版本**: 1.0.0 