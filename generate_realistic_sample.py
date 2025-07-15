#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成更有特色的风控样本数据
包含明显的异常模式和风险特征，便于展示分析效果
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import math

def generate_realistic_sample(n_users=50000, n_transactions=5000000):
    """生成更有特色的样本数据"""
    
    print(f"正在生成 {n_users:,} 用户，{n_transactions:,} 笔交易的特色样本数据...")
    
    # 设置随机种子
    np.random.seed(42)
    random.seed(42)
    
    # 1. 用户分类设计（更有特色）
    user_profiles = {
        'normal_users': {
            'ratio': 0.70,  # 70%正常用户
            'tx_count_range': (5, 50),
            'amount_range': (10, 5000),
            'night_prob': 0.05,  # 5%夜间交易
            'weekend_prob': 0.15,  # 15%周末交易
            'first_digit_bias': None  # 无Benford偏差
        },
        'frequent_users': {
            'ratio': 0.15,  # 15%频繁用户
            'tx_count_range': (100, 500),
            'amount_range': (50, 2000),
            'night_prob': 0.12,
            'weekend_prob': 0.25,
            'first_digit_bias': None
        },
        'high_value_users': {
            'ratio': 0.10,  # 10%高价值用户
            'tx_count_range': (20, 100),
            'amount_range': (5000, 50000),
            'night_prob': 0.08,
            'weekend_prob': 0.20,
            'first_digit_bias': {1: 0.5, 2: 0.3, 3: 0.2}  # 偏向1、2、3开头
        },
        'suspicious_users': {
            'ratio': 0.05,  # 5%可疑用户
            'tx_count_range': (200, 1000),
            'amount_range': (1000, 20000),
            'night_prob': 0.40,  # 40%夜间交易（异常高）
            'weekend_prob': 0.35,  # 35%周末交易
            'first_digit_bias': {4: 0.4, 5: 0.3, 6: 0.3}  # 偏向4、5、6开头（违反Benford）
        }
    }
    
    # 2. 生成用户
    users = []
    user_id = 1
    
    for profile_name, profile in user_profiles.items():
        n_profile_users = int(n_users * profile['ratio'])
        
        for _ in range(n_profile_users):
            users.append({
                'user_id': f'user{user_id:06d}',
                'profile': profile_name,
                'tx_count': random.randint(*profile['tx_count_range']),
                'night_prob': profile['night_prob'],
                'weekend_prob': profile['weekend_prob'],
                'amount_range': profile['amount_range'],
                'first_digit_bias': profile['first_digit_bias']
            })
            user_id += 1
    
    # 3. 生成交易数据
    transactions = []
    tx_types = ['payment', 'transfer', 'withdrawal', 'deposit', 'refund']
    countries = ['CN', 'US', 'JP', 'GB', 'DE', 'FR', 'KR', 'SG', 'AU', 'CA']
    
    # 时间范围
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 1, 1)
    date_range = (end_date - start_date).days
    
    transaction_count = 0
    
    for user in users:
        user_tx_count = min(user['tx_count'], n_transactions - transaction_count)
        
        for _ in range(user_tx_count):
            # 生成时间
            random_days = random.randint(0, date_range)
            tx_date = start_date + timedelta(days=random_days)
            
            # 根据用户特征决定是否夜间/周末交易
            if random.random() < user['night_prob']:
                hour = random.randint(22, 23) if random.random() < 0.5 else random.randint(0, 6)
            else:
                hour = random.randint(7, 21)
            
            if random.random() < user['weekend_prob']:
                # 调整到周末
                days_to_weekend = (5 - tx_date.weekday()) % 7
                if days_to_weekend == 0:
                    days_to_weekend = 6
                tx_date += timedelta(days=days_to_weekend)
            
            # 生成金额（根据用户特征和首位数字偏差）
            if user['first_digit_bias']:
                # 有偏差的用户，按指定概率生成首位数字
                first_digit = np.random.choice(
                    list(user['first_digit_bias'].keys()),
                    p=list(user['first_digit_bias'].values())
                )
                # 生成以特定数字开头的金额
                base_amount = random.uniform(*user['amount_range'])
                amount = float(f"{first_digit}{base_amount:.2f}"[1:])
            else:
                # 正常用户，随机金额
                amount = random.uniform(*user['amount_range'])
            
            # 添加一些随机性
            amount = round(amount + random.uniform(-amount*0.1, amount*0.1), 2)
            amount = max(0.01, amount)  # 确保金额为正
            
            transactions.append({
                'user_id': user['user_id'],
                'op_date': tx_date.strftime('%Y-%m-%d'),
                'op_time': f"{hour:02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
                'amount': amount,
                'tx_type': random.choice(tx_types),
                'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
                'country': random.choice(countries),
                'user_profile': user['profile']
            })
            
            transaction_count += 1
            if transaction_count >= n_transactions:
                break
        
        if transaction_count >= n_transactions:
            break
    
    # 4. 转换为DataFrame并保存
    df = pd.DataFrame(transactions)
    
    # 添加一些极端异常案例（增强展示效果）
    extreme_cases = []
    
    # 添加100个极端夜间大额交易
    for i in range(100):
        extreme_cases.append({
            'user_id': f'extreme{i:03d}',
            'op_date': '2023-06-15',
            'op_time': f"{random.randint(2,4):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            'amount': random.uniform(80000, 200000),  # 极大金额
            'tx_type': 'transfer',
            'ip_address': f"192.168.1.{random.randint(1,255)}",
            'country': 'CN',
            'user_profile': 'extreme_suspicious'
        })
    
    # 添加500个明显违反Benford定律的交易（都以8、9开头）
    for i in range(500):
        first_digit = random.choice([8, 9])
        base_amount = random.uniform(1000, 10000)
        amount = float(f"{first_digit}{base_amount:.2f}"[1:])
        
        extreme_cases.append({
            'user_id': f'benford{i:03d}',
            'op_date': f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            'op_time': f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            'amount': amount,
            'tx_type': 'payment',
            'ip_address': f"10.0.0.{random.randint(1,255)}",
            'country': 'US',
            'user_profile': 'benford_violation'
        })
    
    # 合并极端案例
    extreme_df = pd.DataFrame(extreme_cases)
    df = pd.concat([df, extreme_df], ignore_index=True)
    
    # 打乱顺序
    df = df.sample(frac=1).reset_index(drop=True)
    
    # 保存数据
    df.to_csv('data/sample_tx_log.csv', index=False)
    
    # 5. 生成统计报告
    print("\n=== 特色样本数据生成报告 ===")
    print(f"总交易数: {len(df):,}")
    print(f"总用户数: {df['user_id'].nunique():,}")
    
    # 用户分布
    print("\n用户类型分布:")
    profile_dist = df['user_profile'].value_counts()
    for profile, count in profile_dist.items():
        print(f"  {profile}: {count:,} 笔交易")
    
    # 首位数字分布
    print("\n首位数字分布:")
    df['first_digit'] = df['amount'].astype(str).str[0].astype(int)
    first_digit_dist = df['first_digit'].value_counts().sort_index()
    
    # 计算Benford期望
    benford_expected = {}
    for d in range(1, 10):
        benford_expected[d] = math.log10(1 + 1/d)
    
    print("数字\t实际频率\t期望频率\t偏差")
    print("-" * 40)
    total_count = len(df)
    for digit in range(1, 10):
        actual = first_digit_dist.get(digit, 0) / total_count
        expected = benford_expected[digit]
        deviation = actual - expected
        print(f"{digit}\t{actual:.4f}\t\t{expected:.4f}\t\t{deviation:+.4f}")
    
    # 时间分布
    df['hour'] = pd.to_datetime(df['op_time']).dt.hour
    night_tx = df[(df['hour'] >= 22) | (df['hour'] <= 6)]
    print(f"\n夜间交易比例: {len(night_tx)/len(df)*100:.1f}%")
    
    # 金额分布
    print(f"\n金额统计:")
    print(f"  平均金额: {df['amount'].mean():.2f}")
    print(f"  中位数: {df['amount'].median():.2f}")
    print(f"  最大金额: {df['amount'].max():.2f}")
    print(f"  >10万的交易: {len(df[df['amount'] > 100000])} 笔")
    
    print(f"\n[SUCCESS] 特色样本数据已保存到 data/sample_tx_log.csv")
    print("现在可以运行 python run_full_pipeline_simple.py 查看更有特色的分析结果！")
    
    return df

if __name__ == "__main__":
    generate_realistic_sample() 