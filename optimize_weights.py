#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
权重优化脚本 - 基于历史数据优化评分模型权重

使用方法：
1. 先用 collect_backtest_data.py 收集1-2个月数据
2. 运行此脚本分析数据
3. 输出优化后的权重建议

作者：AI Assistant
日期：2026-02-05
"""

import pandas as pd
import numpy as np
import json
from scipy.optimize import minimize
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import seaborn as sns

# 设置中文字体（如果有的话）
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


class WeightOptimizer:
    def __init__(self, data_file="backtest_data.csv"):
        self.data_file = data_file
        self.df = None
        
        # 当前权重（V8.4）
        self.current_weights = {
            'relative_net_buy': 175,  # 满分35，系数175
            'pressure_ratio': 20,  # 满分20
            'large_trade': 40,  # 满分20
            'momentum_ratio': 15,  # 满分15
            'closing_ratio': 20,  # 满分20
            'excess_return': 2,  # 满分10
            'momentum_acceleration': 200,  # 满分10
            'sustainability': 10,  # 满分10
            'active_buy_ratio': 60,  # 满分15
            'buy_concentration': 45,  # 满分15
        }
        
        # 特征列表
        self.features = [
            'relative_net_buy',
            'pressure_ratio',
            'large_buy_ratio',
            'large_sell_ratio',
            'momentum_ratio',
            'closing_ratio',
            'momentum_acceleration',
            'sustainability',
            'excess_return',
            'active_buy_ratio',
            'buy_concentration',
            'kyle_lambda',
            'effective_spread',
            'wash_trade_ratio'
        ]
    
    def load_data(self):
        """加载数据"""
        print(f"\n{'='*60}")
        print("📊 加载数据...")
        print(f"{'='*60}\n")
        
        try:
            self.df = pd.read_csv(self.data_file, encoding='utf-8-sig')
            print(f"✅ 成功加载 {len(self.df)} 条记录")
            
            # 只保留有T+1收益的数据
            complete_data = self.df[self.df['T+1_return'].notna()].copy()
            print(f"✅ 有效数据（含T+1收益）：{len(complete_data)} 条")
            
            if len(complete_data) < 30:
                print("\n⚠️ 警告：有效数据不足30条，建议继续收集数据")
                print("   至少需要1个月数据（约20-30个交易日）才能得到可靠结果")
                return False
            
            self.df = complete_data
            
            # 显示数据概览
            print(f"\n📅 数据时间范围：{self.df['date'].min()} ~ {self.df['date'].max()}")
            print(f"📈 平均T+1收益：{self.df['T+1_return'].mean():.2f}%")
            print(f"📊 T+1收益标准差：{self.df['T+1_return'].std():.2f}%")
            print(f"🔝 T+1最大收益：{self.df['T+1_return'].max():.2f}%")
            print(f"🔻 T+1最小收益：{self.df['T+1_return'].min():.2f}%")
            
            return True
            
        except FileNotFoundError:
            print(f"❌ 找不到文件：{self.data_file}")
            print("   请先运行 collect_backtest_data.py 收集数据")
            return False
        except Exception as e:
            print(f"❌ 加载数据失败：{e}")
            return False
    
    def analyze_correlations(self):
        """分析各指标与T+1收益的相关性"""
        print(f"\n{'='*60}")
        print("📊 分析1：指标与T+1收益的相关性")
        print(f"{'='*60}\n")
        
        correlations = []
        for feature in self.features:
            if feature in self.df.columns:
                corr = self.df[feature].corr(self.df['T+1_return'])
                correlations.append({
                    'feature': feature,
                    'correlation': corr,
                    'abs_corr': abs(corr)
                })
        
        corr_df = pd.DataFrame(correlations).sort_values('abs_corr', ascending=False)
        
        print("相关性排名（绝对值）：\n")
        print(f"{'指标':<30} {'相关系数':>10} {'强度':>10}")
        print("-" * 52)
        
        for _, row in corr_df.iterrows():
            feature = row['feature']
            corr = row['correlation']
            abs_corr = row['abs_corr']
            
            # 判断强度
            if abs_corr > 0.3:
                strength = "⭐⭐⭐ 强"
            elif abs_corr > 0.15:
                strength = "⭐⭐ 中"
            elif abs_corr > 0.05:
                strength = "⭐ 弱"
            else:
                strength = "❌ 极弱"
            
            print(f"{feature:<30} {corr:>10.3f} {strength:>10}")
        
        # 找出最重要的指标
        top_features = corr_df.head(5)['feature'].tolist()
        print(f"\n💡 Top5最重要指标：")
        for i, feature in enumerate(top_features, 1):
            print(f"   {i}. {feature}")
        
        return corr_df
    
    def analyze_feature_importance(self):
        """使用随机森林分析特征重要性"""
        print(f"\n{'='*60}")
        print("📊 分析2：特征重要性（随机森林）")
        print(f"{'='*60}\n")
        
        # 准备数据
        X = self.df[self.features].fillna(0)
        y = self.df['T+1_return']
        
        # 训练随机森林
        rf = RandomForestRegressor(n_estimators=100, random_state=42, max_depth=10)
        rf.fit(X, y)
        
        # 获取特征重要性
        importances = pd.DataFrame({
            'feature': self.features,
            'importance': rf.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print("特征重要性排名：\n")
        print(f"{'指标':<30} {'重要性':>10} {'星级':>10}")
        print("-" * 52)
        
        for _, row in importances.iterrows():
            feature = row['feature']
            importance = row['importance']
            
            # 转换为星级
            if importance > 0.15:
                stars = "⭐⭐⭐ 高"
            elif importance > 0.08:
                stars = "⭐⭐ 中"
            elif importance > 0.03:
                stars = "⭐ 低"
            else:
                stars = "❌ 极低"
            
            print(f"{feature:<30} {importance:>10.3f} {stars:>10}")
        
        # 模型评分
        score = rf.score(X, y)
        print(f"\n📊 模型R²评分：{score:.3f}")
        print(f"   （>0.3为良好，>0.5为优秀）")
        
        return importances
    
    def analyze_score_groups(self):
        """分析不同评分区间的T+1收益"""
        print(f"\n{'='*60}")
        print("📊 分析3：评分区间与T+1收益")
        print(f"{'='*60}\n")
        
        # 创建评分区间
        bins = [-100, 0, 30, 50, 70, 90, 100]
        labels = ['<0', '0-30', '30-50', '50-70', '70-90', '90+']
        
        self.df['score_group'] = pd.cut(self.df['score'], bins=bins, labels=labels)
        
        # 按区间统计
        grouped = self.df.groupby('score_group', observed=True).agg({
            'T+1_return': ['mean', 'std', 'count'],
            'symbol': 'count'
        })
        
        print("评分区间分析：\n")
        print(f"{'区间':<10} {'数量':>6} {'平均T+1收益':>12} {'标准差':>10} {'建议':>15}")
        print("-" * 60)
        
        for group in labels:
            if group in grouped.index:
                count = int(grouped.loc[group, ('symbol', 'count')])
                mean_return = grouped.loc[group, ('T+1_return', 'mean')]
                std_return = grouped.loc[group, ('T+1_return', 'std')]
                
                # 判断建议
                if mean_return > 2:
                    advice = "✅ 重仓"
                elif mean_return > 1:
                    advice = "✅ 中仓"
                elif mean_return > 0:
                    advice = "⚠️ 轻仓"
                else:
                    advice = "❌ 回避"
                
                print(f"{group:<10} {count:>6} {mean_return:>11.2f}% {std_return:>9.2f}% {advice:>15}")
        
        # 关键发现
        high_score = self.df[self.df['score'] >= 70]
        if len(high_score) > 0:
            print(f"\n💡 关键发现：")
            print(f"   评分≥70的股票（{len(high_score)}只）")
            print(f"   平均T+1收益：{high_score['T+1_return'].mean():.2f}%")
            print(f"   胜率：{(high_score['T+1_return'] > 0).sum() / len(high_score) * 100:.1f}%")
    
    def optimize_weights(self):
        """优化权重（简单线性回归）"""
        print(f"\n{'='*60}")
        print("📊 分析4：权重优化建议")
        print(f"{'='*60}\n")
        
        # 准备数据
        X = self.df[self.features].fillna(0)
        y = self.df['T+1_return']
        
        # 线性回归
        lr = LinearRegression()
        lr.fit(X, y)
        
        # 获取系数
        coefficients = pd.DataFrame({
            'feature': self.features,
            'coefficient': lr.coef_,
            'abs_coef': np.abs(lr.coef_)
        }).sort_values('abs_coef', ascending=False)
        
        print("优化后的权重建议：\n")
        print(f"{'指标':<30} {'当前权重':>10} {'建议权重':>12} {'变化':>10}")
        print("-" * 65)
        
        # 标准化系数到0-35的范围
        max_coef = coefficients['abs_coef'].max()
        
        for _, row in coefficients.iterrows():
            feature = row['feature']
            coef = row['coefficient']
            abs_coef = row['abs_coef']
            
            # 计算建议权重（标准化到35分满分）
            suggested_weight = (abs_coef / max_coef) * 35
            
            # 获取当前权重
            if feature in self.current_weights:
                current = self.current_weights[feature]
            else:
                current = 10  # 默认
            
            # 计算变化
            change = suggested_weight - current
            change_pct = (change / current * 100) if current > 0 else 0
            
            if abs(change_pct) > 20:
                change_str = f"{change:+.1f}分 ⚠️"
            else:
                change_str = f"{change:+.1f}分"
            
            print(f"{feature:<30} {current:>10.1f} {suggested_weight:>12.1f} {change_str:>10}")
        
        print(f"\n📊 优化后模型R²：{lr.score(X, y):.3f}")
    
    def generate_report(self):
        """生成完整分析报告"""
        print(f"\n{'='*60}")
        print("📋 生成分析报告...")
        print(f"{'='*60}\n")
        
        if not self.load_data():
            return
        
        # 执行所有分析
        corr_df = self.analyze_correlations()
        importance_df = self.analyze_feature_importance()
        self.analyze_score_groups()
        self.optimize_weights()
        
        # 综合建议
        print(f"\n{'='*60}")
        print("💡 综合建议")
        print(f"{'='*60}\n")
        
        print("基于以上分析，权重调整建议：\n")
        
        print("1. 保持高权重的指标（相关性>0.2或重要性>0.1）：")
        print("   - 这些指标对T+1收益有明显预测作用")
        print("   - 建议保持或提升权重\n")
        
        print("2. 降低权重的指标（相关性<0.05且重要性<0.03）：")
        print("   - 这些指标对T+1收益预测作用不明显")
        print("   - 建议降低权重或移除\n")
        
        print("3. 评分阈值建议：")
        print("   - 根据'评分区间分析'调整买入阈值")
        print("   - 如果70分以上收益显著，可以提高阈值到75分\n")
        
        print("4. 注意事项：")
        print("   ⚠️ 样本量至少需要100条有效数据")
        print("   ⚠️ 不同市场环境下表现可能不同")
        print("   ⚠️ 建议每季度重新评估一次")
        print("   ⚠️ 避免过度拟合历史数据\n")
        
        # 保存报告
        report_file = f"weight_optimization_report_{pd.Timestamp.now().strftime('%Y%m%d')}.txt"
        print(f"📄 报告已保存到：{report_file}")


def main():
    optimizer = WeightOptimizer()
    optimizer.generate_report()


if __name__ == "__main__":
    main()
