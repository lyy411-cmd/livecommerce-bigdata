<template>
  <div class="arch-page">
    <div class="arch-hero">
      <h1>星播大数据分析平台</h1>
      <h2>系统架构与数据流转</h2>
      <p>展示所有大数据组件在项目中的实际用途</p>
    </div>

    <!-- 组件卡片网格 -->
    <div class="arch-grid">
      <div class="arch-card kafka">
        <div class="card-icon">
          <div class="icon-box"><el-icon :size="36"><Connection /></el-icon></div>
        </div>
        <div class="card-body">
          <h3>Kafka</h3>
          <p class="card-subtitle">消息队列 | 虚拟机 192.168.104.100:9092</p>
          <ul>
            <li><strong>接收直播数据流</strong></li>
            <li>实时弹幕消息</li>
            <li>用户行为埋点</li>
            <li>订单事件推送</li>
            <li>直播间状态变更</li>
          </ul>
          <div class="topic-tags">
            <el-tag size="small">live_room_events</el-tag>
            <el-tag size="small" type="success">order_events</el-tag>
            <el-tag size="small" type="warning">user_behavior</el-tag>
          </div>
        </div>
      </div>

      <div class="arch-card flink">
        <div class="card-icon">
          <div class="icon-box"><el-icon :size="36"><DataLine /></el-icon></div>
        </div>
        <div class="card-body">
          <h3>Flink</h3>
          <p class="card-subtitle">实时流计算 | 虚拟机 192.168.104.100:8081</p>
          <ul>
            <li><strong>实时计算</strong></li>
            <li>在线人数统计</li>
            <li>热门商品排行</li>
            <li>弹幕情感分析</li>
            <li>异常行为告警</li>
          </ul>
          <el-tag size="small" effect="dark">3 个作业运行中</el-tag>
        </div>
      </div>

      <div class="arch-card hive">
        <div class="card-icon">
          <div class="icon-box"><el-icon :size="36"><Coin /></el-icon></div>
        </div>
        <div class="card-body">
          <h3>Hive</h3>
          <p class="card-subtitle">数据仓库 | 虚拟机 192.168.104.100:10000</p>
          <ul>
            <li><strong>存储历史数据</strong></li>
            <li>主播收益汇总</li>
            <li>用户画像构建</li>
            <li>日/周/月报表</li>
            <li>趋势分析数据</li>
          </ul>
          <el-tag size="small" type="info" effect="dark">ODS -> DWD -> DWS -> ADS</el-tag>
        </div>
      </div>

      <div class="arch-card hdfs">
        <div class="card-icon">
          <div class="icon-box"><el-icon :size="36"><Folder /></el-icon></div>
        </div>
        <div class="card-body">
          <h3>Hadoop HDFS</h3>
          <p class="card-subtitle">分布式文件系统 | 虚拟机 192.168.104.100:9870</p>
          <ul>
            <li><strong>存储原始数据</strong></li>
            <li>直播视频回放</li>
            <li>原始日志归档</li>
            <li>Parquet 文件存储</li>
            <li>数据备份</li>
          </ul>
        </div>
      </div>

      <div class="arch-card mysql">
        <div class="card-icon">
          <div class="icon-box"><el-icon :size="36"><Monitor /></el-icon></div>
        </div>
        <div class="card-body">
          <h3>MySQL</h3>
          <p class="card-subtitle">业务数据库 | 虚拟机 192.168.104.100:3306</p>
          <ul>
            <li><strong>存储业务数据</strong></li>
            <li>用户与权限信息</li>
            <li>主播与直播间数据</li>
            <li>订单与交易记录</li>
            <li>运营配置</li>
          </ul>
          <el-tag size="small" type="success" effect="dark">livecommerce_db</el-tag>
        </div>
      </div>

      <div class="arch-card python">
        <div class="card-icon">
          <div class="icon-box"><el-icon :size="36"><Platform /></el-icon></div>
        </div>
        <div class="card-body">
          <h3>Python</h3>
          <p class="card-subtitle">数据采集 + AI分析 + Web后端</p>
          <ul>
            <li><strong>多功能后端</strong></li>
            <li>RESTful API 服务</li>
            <li>数据采集调度</li>
            <li>数据清洗管道</li>
            <li>模拟数据生成</li>
          </ul>
          <el-tag size="small" effect="dark">run_cluster.py + Flask API</el-tag>
        </div>
      </div>

      <div class="arch-card zookeeper">
        <div class="card-icon">
          <div class="icon-box"><el-icon :size="36"><Setting /></el-icon></div>
        </div>
        <div class="card-body">
          <h3>Zookeeper</h3>
          <p class="card-subtitle">分布式协调服务 | 虚拟机 2181</p>
          <ul>
            <li><strong>集群协调</strong></li>
            <li>Kafka Broker 管理</li>
            <li>Leader 选举</li>
            <li>配置同步</li>
          </ul>
        </div>
      </div>

      <div class="arch-card yarn">
        <div class="card-icon">
          <div class="icon-box"><el-icon :size="36"><Operation /></el-icon></div>
        </div>
        <div class="card-body">
          <h3>YARN</h3>
          <p class="card-subtitle">资源管理 | Hadoop 集群</p>
          <ul>
            <li><strong>资源调度</strong></li>
            <li>Flink 任务资源分配</li>
            <li>Hive 查询资源管理</li>
            <li>MapReduce 作业调度</li>
          </ul>
        </div>
      </div>
    </div>

    <!-- 数据流转图 -->
    <div class="flow-section">
      <h2>数据流转全链路</h2>
      <div class="flow-visual">
        <div class="flow-row">
          <div class="flow-step source">
            <div class="step-label">数据源</div>
            <div class="step-items">
              <span class="step-item">直播弹幕</span>
              <span class="step-item">用户行为</span>
              <span class="step-item">订单事件</span>
            </div>
          </div>
          <div class="flow-arrow">
            <el-icon :size="24"><DArrowRight /></el-icon>
            <span>Push</span>
          </div>
          <div class="flow-step transport">
            <div class="step-label">Kafka</div>
            <div class="step-items">
              <span class="step-item">Topic 分区</span>
              <span class="step-item">消息持久化</span>
              <span class="step-item">流式推送</span>
            </div>
          </div>
          <div class="flow-arrow">
            <el-icon :size="24"><DArrowRight /></el-icon>
            <span>Consume</span>
          </div>
          <div class="flow-step compute">
            <div class="step-label">Flink</div>
            <div class="step-items">
              <span class="step-item">实时清洗</span>
              <span class="step-item">聚合计算</span>
              <span class="step-item">异常告警</span>
            </div>
          </div>
          <div class="flow-arrow">
            <el-icon :size="24"><DArrowRight /></el-icon>
            <span>Sink</span>
          </div>
          <div class="flow-step storage">
            <div class="step-label">存储层</div>
            <div class="step-items">
              <span class="step-item">MySQL</span>
              <span class="step-item">Hive</span>
              <span class="step-item">HDFS</span>
            </div>
          </div>
        </div>
        <div class="flow-row api-row">
          <div class="flow-arrow" style="flex-direction: column;">
            <el-icon :size="24"><Bottom /></el-icon>
            <span>REST API</span>
          </div>
        </div>
        <div class="flow-row">
          <div class="flow-step display" style="width:300px">
            <div class="step-label">展示层</div>
            <div class="step-items">
              <span class="step-item">Vue 3 Dashboard</span>
              <span class="step-item">ECharts 图表</span>
              <span class="step-item">实时大屏</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 组件详细用途表格 -->
    <div class="table-section">
      <h2>组件详细用途说明</h2>
      <el-table :data="components" border stripe style="width:100%">
        <el-table-column prop="name" label="组件" width="120" align="center">
          <template #default="{ row }">
            <el-tag :style="{ background: row.color, color: '#fff', border: 'none' }" size="large">
              {{ row.name }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="usage" label="用途" min-width="400">
          <template #default="{ row }">
            <div v-for="u in row.usage" :key="u" class="usage-item">
              <el-icon color="#52c41a"><Select /></el-icon>
              <span>{{ u }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="address" label="连接地址" width="280" />
        <el-table-column prop="status" label="状态" width="120" align="center">
          <template #default="{ row }">
            <el-tag :type="row.status === 'connected' ? 'success' : 'warning'" size="small" effect="dark">
              {{ row.status === 'connected' ? '已连接' : '未知' }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'

const components = ref([
  {
    name: 'Kafka', color: '#1890ff',
    usage: ['接收直播数据流', '弹幕消息', '用户行为埋点', '订单事件推送', '直播间状态变更'],
    address: '192.168.104.100:9092', status: 'connected'
  },
  {
    name: 'Flink', color: '#e06c75',
    usage: ['实时计算', '在线人数统计', '热门商品排行', '弹幕情感分析', '异常行为告警'],
    address: '192.168.104.100:8081', status: 'connected'
  },
  {
    name: 'Hive', color: '#fab005',
    usage: ['存储历史数据', '主播收益汇总', '用户画像构建', '日报/周报/月报', '趋势分析数据'],
    address: '192.168.104.100:10000', status: 'connected'
  },
  {
    name: 'HDFS', color: '#40c057',
    usage: ['分布式文件存储', '直播视频回放', '原始日志归档', 'Parquet 文件', '数据备份'],
    address: '192.168.104.100:9870', status: 'connected'
  },
  {
    name: 'MySQL', color: '#15aabf',
    usage: ['业务数据存储', '用户权限信息', '主播直播间数据', '订单交易记录', '运营配置'],
    address: '192.168.104.100:3306', status: 'connected'
  },
  {
    name: 'Python', color: '#4c6ef5',
    usage: ['Web 后端 API', 'RESTful 服务', '数据采集调度', 'AI 数据分析', '模拟数据生成'],
    address: 'localhost:8080', status: 'connected'
  },
  {
    name: 'Zookeeper', color: '#7950f2',
    usage: ['集群协调', 'Kafka Broker 管理', 'Leader 选举', '配置同步'],
    address: '192.168.104.100:2181', status: 'connected'
  },
  {
    name: 'YARN', color: '#fa5252',
    usage: ['资源调度', 'Flink 任务资源', 'Hive 查询资源', 'MapReduce 作业'],
    address: '192.168.104.100:8088', status: 'connected'
  }
])
</script>

<style scoped lang="scss">
.arch-page {
  display: flex; flex-direction: column; gap: 24px; padding-bottom: 40px; overflow-y: auto; height: 100%;
}
.arch-hero {
  background: linear-gradient(135deg, #0a1628 0%, #0d2040 50%, #0a2a3a 100%);
  color: #e0e0e0; border-radius: 12px; padding: 32px; text-align: center;
  border: 1px solid rgba(0, 255, 204, 0.12);
  box-shadow: 0 4px 20px rgba(0, 255, 204, 0.06);
  h1 { font-size: 26px; margin: 0; color: #00ffcc; }
  h2 { font-size: 18px; margin: 8px 0; opacity: 0.85; font-weight: 400; }
  p { font-size: 13px; opacity: 0.5; margin: 4px 0 0; }
}

.arch-grid {
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px;
}
@media (max-width: 1600px) { .arch-grid { grid-template-columns: repeat(3, 1fr); } }
@media (max-width: 1200px) { .arch-grid { grid-template-columns: repeat(2, 1fr); } }

.arch-card {
  background: rgba(15, 20, 30, 0.55); border-radius: 10px; padding: 20px;
  border: 1px solid rgba(255,255,255,0.06);
  border-top: 4px solid #555; transition: all 0.3s; position: relative; overflow: hidden;
  backdrop-filter: blur(10px);
  &::after { content: ''; position: absolute; right: -30px; top: -30px; width: 100px; height: 100px; border-radius: 50%; opacity: 0.06; }
  &:hover { transform: translateY(-3px); border-color: rgba(0, 255, 204, 0.2); box-shadow: 0 4px 20px rgba(0, 255, 204, 0.08); }
  &.kafka { border-top-color: #00d9ff; &::after { background: #00d9ff; } .icon-box { background: rgba(0, 217, 255, 0.12); color: #00d9ff; } }
  &.flink { border-top-color: #e06c75; &::after { background: #e06c75; } .icon-box { background: rgba(224, 108, 117, 0.12); color: #e06c75; } }
  &.hive { border-top-color: #fab005; &::after { background: #fab005; } .icon-box { background: rgba(250, 176, 5, 0.12); color: #fab005; } }
  &.hdfs { border-top-color: #40c057; &::after { background: #40c057; } .icon-box { background: rgba(64, 192, 87, 0.12); color: #40c057; } }
  &.mysql { border-top-color: #15aabf; &::after { background: #15aabf; } .icon-box { background: rgba(21, 170, 191, 0.12); color: #15aabf; } }
  &.python { border-top-color: #4c6ef5; &::after { background: #4c6ef5; } .icon-box { background: rgba(76, 110, 245, 0.12); color: #4c6ef5; } }
  &.zookeeper { border-top-color: #7950f2; &::after { background: #7950f2; } .icon-box { background: rgba(121, 80, 242, 0.12); color: #7950f2; } }
  &.yarn { border-top-color: #fa5252; &::after { background: #fa5252; } .icon-box { background: rgba(250, 82, 82, 0.12); color: #fa5252; } }
}

.card-icon { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px; }
.icon-box { width: 52px; height: 52px; border-radius: 12px; display: flex; align-items: center; justify-content: center; }
.card-body {
  h3 { font-size: 17px; font-weight: 700; color: #e0e0e0; margin: 0; }
  .card-subtitle { font-size: 12px; color: rgba(255,255,255,0.4); margin: 6px 0 10px; }
  ul { margin: 0; padding-left: 18px; li { font-size: 13px; color: rgba(255,255,255,0.55); margin-bottom: 4px; } }
}
.topic-tags { margin-top: 8px; display: flex; gap: 4px; flex-wrap: wrap; }

.flow-section, .table-section {
  background: rgba(15, 20, 30, 0.5);
  border: 1px solid rgba(0, 255, 204, 0.08);
  border-radius: 10px;
  padding: 24px;
  backdrop-filter: blur(10px);
  h2 { margin: 0 0 16px; font-size: 18px; color: #e0e0e0; }
}
.flow-visual { display: flex; flex-direction: column; align-items: center; gap: 8px; }
.flow-row { display: flex; align-items: center; gap: 12px; }
.flow-step {
  background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.06); border-radius: 8px;
  padding: 16px 20px; text-align: center; min-width: 150px;
  .step-label { font-size: 16px; font-weight: 700; color: #e0e0e0; margin-bottom: 8px; }
  .step-items { display: flex; flex-wrap: wrap; gap: 4px; justify-content: center; }
  .step-item { padding: 3px 10px; border-radius: 12px; font-size: 12px; }
  &.source .step-item { background: rgba(0, 217, 255, 0.12); color: #00d9ff; }
  &.transport .step-item { background: rgba(0, 217, 255, 0.1); color: #00d9ff; }
  &.compute .step-item { background: rgba(224, 108, 117, 0.12); color: #f0949a; }
  &.storage .step-item { background: rgba(64, 192, 87, 0.12); color: #69db7c; }
  &.display .step-item { background: rgba(121, 80, 242, 0.12); color: #a78bfa; }
}
.flow-arrow { display: flex; flex-direction: column; align-items: center; gap: 2px; color: rgba(255,255,255,0.35); font-size: 10px; }
.api-row { justify-content: center; }

.usage-item { display: flex; align-items: center; gap: 8px; padding: 4px 0; font-size: 13px; color: rgba(255,255,255,0.55); }
</style>
