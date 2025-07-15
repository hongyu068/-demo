#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版风险监测系统完整流程
使用pandas替代PySpark，避免Python版本不匹配问题
"""

import os
import sys
import pandas as pd
import numpy as np
import math
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json

def load_sample_data():
    """加载样本数据"""
    try:
        if os.path.exists('data/sample_tx_log.csv'):
            df = pd.read_csv('data/sample_tx_log.csv')
            print(f"[OK] 加载样本数据成功，共 {len(df):,} 条记录")
            return df
        else:
            print("[FAIL] 未找到样本数据文件")
            return None
    except Exception as e:
        print(f"[FAIL] 加载数据失败: {e}")
        return None

def clean_data(df):
    """数据清洗"""
    try:
        print("正在清洗数据...")
        
        # 合并日期和时间
        df['op_datetime'] = pd.to_datetime(df['op_date'] + ' ' + df['op_time'])
        
        # 过滤无效数据
        df = df.dropna(subset=['user_id', 'amount', 'op_datetime'])
        df = df[df['amount'] > 0]
        df = df[df['amount'] <= 1000000]  # 过滤异常高金额
        
        # 添加时间特征
        df['hour'] = df['op_datetime'].dt.hour
        df['day_of_week'] = df['op_datetime'].dt.dayofweek
        df['is_night'] = ((df['hour'] >= 22) | (df['hour'] <= 6)).astype(int)
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # 添加金额特征
        df['amount_log'] = np.log10(df['amount'])
        df['amount_category'] = pd.cut(df['amount'], 
                                     bins=[0, 1000, 10000, 100000, float('inf')],
                                     labels=['small', 'medium', 'large', 'very_large'])
        
        print(f"[OK] 数据清洗完成，保留 {len(df):,} 条记录")
        return df
        
    except Exception as e:
        print(f"[FAIL] 数据清洗失败: {e}")
        return None

def extract_features(df):
    """特征工程"""
    try:
        print("正在提取特征...")
        
        # 提取首位数字 - 修复小数点问题
        df['first_digit'] = df['amount'].astype(str).str.replace('.', '').str[0].astype(int)
        df['first_two_digits'] = df['amount'].astype(str).str.replace('.', '').str[:2].astype(int)
        df['amount_digits'] = df['amount'].astype(str).str.replace('.', '').str.len()
        
        # 时间段分类
        df['hour_period'] = pd.cut(df['hour'], 
                                 bins=[0, 6, 12, 18, 24], 
                                 labels=['night', 'morning', 'afternoon', 'evening'])
        
        print("[OK] 特征提取完成")
        return df
        
    except Exception as e:
        print(f"[FAIL] 特征提取失败: {e}")
        return None

def benford_analysis(df):
    """Benford定律分析"""
    try:
        print("正在进行Benford分析...")
        
        # 计算首位数字分布
        first_digit_dist = df['first_digit'].value_counts().sort_index()
        total_count = len(df)
        
        # Benford期望频率
        benford_expected = {}
        for d in range(1, 10):
            benford_expected[d] = math.log10(1 + 1/d)
        
        # 计算实际频率
        actual_freq = first_digit_dist / total_count
        
        # 计算chi2统计量
        chi2_stat = 0
        for digit in range(1, 10):
            expected = benford_expected[digit] * total_count
            actual = first_digit_dist.get(digit, 0)
            if expected > 0:
                chi2_stat += (actual - expected) ** 2 / expected
        
        # 计算MAD
        mad = 0
        for digit in range(1, 10):
            expected = benford_expected[digit]
            actual = actual_freq.get(digit, 0)
            mad += abs(actual - expected)
        mad /= 9
        
        # 风险评分
        risk_score = min(chi2_stat / 15.0, 1.0)  # 标准化到0-1
        
        print(f"[OK] Benford分析完成")
        print(f"chi2统计量: {chi2_stat:.4f}")
        print(f"MAD: {mad:.6f}")
        print(f"风险评分: {risk_score:.4f}")
        
        return {
            'chi2_stat': chi2_stat,
            'mad': mad,
            'risk_score': risk_score,
            'first_digit_dist': first_digit_dist,
            'actual_freq': actual_freq,
            'expected_freq': benford_expected
        }
        
    except Exception as e:
        print(f"[FAIL] Benford分析失败: {e}")
        return None

def clustering_analysis(df):
    """聚类分析"""
    try:
        print("正在进行聚类分析...")
        
        # 用户行为特征
        user_features = df.groupby('user_id').agg({
            'amount': ['count', 'mean', 'std', 'sum'],
            'is_night': 'sum',
            'is_weekend': 'sum',
            'hour': 'mean'
        }).reset_index()
        
        user_features.columns = ['user_id', 'tx_count', 'avg_amount', 'std_amount', 
                               'total_amount', 'night_count', 'weekend_count', 'avg_hour']
        
        # 简单的风险评分
        user_features['risk_score'] = (
            user_features['night_count'] / user_features['tx_count'] * 0.3 +
            user_features['weekend_count'] / user_features['tx_count'] * 0.2 +
            (user_features['std_amount'] / user_features['avg_amount']).fillna(0) * 0.5
        )
        
        # 风险等级
        user_features['risk_level'] = pd.cut(user_features['risk_score'],
                                           bins=[0, 0.3, 0.7, 1.0],
                                           labels=['低风险', '中风险', '高风险'])
        
        print(f"[OK] 聚类分析完成，共 {len(user_features)} 个用户")
        return user_features
        
    except Exception as e:
        print(f"[FAIL] 聚类分析失败: {e}")
        return None

def identify_anomalies(df, user_features):
    """识别异常用户"""
    try:
        print("正在识别异常用户...")
        
        # 高风险用户
        high_risk_users = user_features[user_features['risk_score'] > 0.7]
        
        # 异常交易
        anomaly_threshold = df['amount'].quantile(0.99)
        suspicious_tx = df[df['amount'] > anomaly_threshold]
        
        print(f"[OK] 异常识别完成")
        print(f"高风险用户: {len(high_risk_users)} 个")
        print(f"异常交易: {len(suspicious_tx)} 笔")
        
        return high_risk_users, suspicious_tx
        
    except Exception as e:
        print(f"[FAIL] 异常识别失败: {e}")
        return None, None

def create_dashboard(df, benford_results, user_features, high_risk_users):
    """创建仪表盘"""
    try:
        print("正在创建仪表盘...")
        
        # 1. Benford热力图
        benford_data = []
        for digit in range(1, 10):
            benford_data.append([
                digit,
                benford_results['actual_freq'].get(digit, 0),
                benford_results['expected_freq'][digit]
            ])
        
        benford_df = pd.DataFrame(benford_data, columns=['digit', 'actual', 'expected'])
        
        # 2. 用户聚类散点图
        cluster_data = user_features[['avg_amount', 'tx_count', 'risk_score']].copy()
        cluster_data['cluster'] = pd.cut(cluster_data['risk_score'], 
                                       bins=3, labels=['低风险', '中风险', '高风险'])
        
        # 3. 风险分布
        risk_dist = user_features['risk_level'].value_counts()
        
        # 4. 汇总指标
        summary_metrics = {
            'total_users': len(user_features),
            'anomaly_users': len(high_risk_users),
            'anomaly_rate': len(high_risk_users) / len(user_features),
            'avg_risk_score': user_features['risk_score'].mean()
        }
        
        # 创建Plotly仪表盘
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Benford定律分析', '用户聚类分布', '异常用户分类', '风险评分分布'),
            specs=[[{"type": "bar"}, {"type": "scatter"}],
                   [{"type": "bar"}, {"type": "histogram"}]]
        )
        
        # Benford分析图
        fig.add_trace(
            go.Bar(x=benford_df['digit'], y=benford_df['actual'], name='实际频率'),
            row=1, col=1
        )
        fig.add_trace(
            go.Bar(x=benford_df['digit'], y=benford_df['expected'], name='期望频率'),
            row=1, col=1
        )
        
        # 聚类散点图
        fig.add_trace(
            go.Scatter(x=cluster_data['avg_amount'], y=cluster_data['tx_count'],
                      mode='markers', marker=dict(color=cluster_data['risk_score']),
                      text=cluster_data['cluster'], name='用户聚类'),
            row=1, col=2
        )
        
        # 异常用户分类
        fig.add_trace(
            go.Bar(x=risk_dist.index, y=risk_dist.values, name='风险分布'),
            row=2, col=1
        )
        
        # 风险评分分布
        fig.add_trace(
            go.Histogram(x=user_features['risk_score'], name='风险评分分布'),
            row=2, col=2
        )
        
        fig.update_layout(height=800, title_text="风险监测仪表盘")
        
        # 保存仪表盘
        fig.write_html('dashboard/simple_dashboard.html')
        
        # 保存数据
        benford_df.to_csv('data/simple_benford_results.csv', index=False)
        cluster_data.to_csv('data/simple_clustering_results.csv', index=False)
        user_features.to_csv('data/simple_user_features.csv', index=False)
        high_risk_users.to_csv('data/simple_anomaly_users.csv', index=False)
        
        # 保存汇总指标
        with open('data/simple_summary.json', 'w', encoding='utf-8') as f:
            json.dump(summary_metrics, f, ensure_ascii=False, indent=2)
        
        print("[OK] 仪表盘创建完成")
        return True
        
    except Exception as e:
        print(f"[FAIL] 仪表盘创建失败: {e}")
        return False

def generate_report(benford_results, user_features, high_risk_users):
    """生成报告"""
    try:
        print("正在生成报告...")
        
        report = f"""
# 风险监测系统报告
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 1. 数据概览
- 总用户数: {len(user_features):,}
- 异常用户数: {len(high_risk_users):,}
- 异常率: {len(high_risk_users)/len(user_features)*100:.2f}%

## 2. Benford分析结果
- chi2统计量: {benford_results['chi2_stat']:.4f}
- MAD: {benford_results['mad']:.6f}
- 风险评分: {benford_results['risk_score']:.4f}

## 3. 风险分布
{user_features['risk_level'].value_counts().to_string()}

## 4. 高风险用户特征
- 平均交易次数: {high_risk_users['tx_count'].mean():.1f}
- 平均交易金额: {high_risk_users['avg_amount'].mean():.2f}
- 夜间操作比例: {high_risk_users['night_count'].sum()/high_risk_users['tx_count'].sum()*100:.1f}%

## 5. 建议
1. 重点关注高风险用户群体
2. 加强夜间和周末交易监控
3. 定期进行Benford分析检测数据异常
        """
        
        with open('reports/simple_risk_report.md', 'w', encoding='utf-8') as f:
            f.write(report)
        
        print("[OK] 报告生成完成")
        return True
        
    except Exception as e:
        print(f"[FAIL] 报告生成失败: {e}")
        return False

def main():
    """主函数"""
    print("=== 简化版风险监测系统完整流程 ===")
    start_time = datetime.now()
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. 加载数据
        print("\n步骤1: 加载数据")
        df = load_sample_data()
        if df is None:
            return
        
        # 2. 数据清洗
        print("\n步骤2: 数据清洗")
        df_cleaned = clean_data(df)
        if df_cleaned is None:
            return
        
        # 3. 特征工程
        print("\n步骤3: 特征工程")
        df_features = extract_features(df_cleaned)
        if df_features is None:
            return
        
        # 4. Benford分析
        print("\n步骤4: Benford分析")
        benford_results = benford_analysis(df_features)
        if benford_results is None:
            return
        
        # 5. 聚类分析
        print("\n步骤5: 聚类分析")
        user_features = clustering_analysis(df_features)
        if user_features is None:
            return
        
        # 6. 异常识别
        print("\n步骤6: 异常识别")
        high_risk_users, suspicious_tx = identify_anomalies(df_features, user_features)
        if high_risk_users is None:
            return
        
        # 7. 创建仪表盘
        print("\n步骤7: 创建仪表盘")
        dashboard_success = create_dashboard(df_features, benford_results, user_features, high_risk_users)
        if not dashboard_success:
            return
        
        # 8. 生成报告
        print("\n步骤8: 生成报告")
        report_success = generate_report(benford_results, user_features, high_risk_users)
        if not report_success:
            return
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n=== 流程完成 ===")
        print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总耗时: {duration}")
        print(f"\n输出文件:")
        print(f"- dashboard/simple_dashboard.html (简化版仪表盘)")
        print(f"- reports/simple_risk_report.md (风险报告)")
        print(f"- data/simple_anomaly_users.csv (异常用户清单)")
        
        print(f"\n[SUCCESS] 简化版风险监测系统运行成功！")
        
    except Exception as e:
        print(f"\n[FAIL] 流程执行失败: {e}")
        return

if __name__ == "__main__":
    main() 