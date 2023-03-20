# AWS Clean Rooms Demo: 

这个demo提供了两个案例，分别用于帮助体验List(非聚合)和Aggregation(聚合)两种合作方式

## Setup

这个demo要求提供两个accounts， 其中一个为数据贡献者，另外一个是数据受益者

并且所有的数据需要在同一个region

构建步骤:

1. 登录其中一个account, 创建cloud9环境，更新 ~/.aws/credentials (添加两个账号的AK/SK)
2. 更新enhanced_mock.py中的两个account的profile信息
3. 运行脚本 `python3 enhanced_mock.py`
4. 检查两个账户中的数据创建情况
