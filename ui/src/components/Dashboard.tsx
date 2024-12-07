import React, { useEffect, useState } from "react";
import { Card, Row, Col, Statistic } from "antd";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";
import { invoke } from "@tauri-apps/api/core";

interface SystemMetrics {
  cpu_usage: number;
  memory_usage: number;
  disk_usage: number;
  timestamp: number;
}

export const Dashboard: React.FC = () => {
  const [metrics, setMetrics] = useState<SystemMetrics[]>([]);
  const [currentMetrics, setCurrentMetrics] = useState<SystemMetrics | null>(
    null
  );

  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        const result = await invoke<SystemMetrics>("get_system_metrics");
        console.log("Metrics:", result);
        setCurrentMetrics(result);
        setMetrics((prev) => [...prev, result].slice(-30));
      } catch (error) {
        console.error("Failed to fetch metrics:", error);
      }
    };

    fetchMetrics();
    const interval = setInterval(fetchMetrics, 1000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div style={{ padding: "24px" }}>
      <Row gutter={[16, 16]}>
        <Col span={8}>
          <Card>
            <Statistic
              title="CPU Usage"
              value={currentMetrics?.cpu_usage ?? 0}
              suffix="%"
              precision={1}
            />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <Statistic
              title="Memory Usage"
              value={currentMetrics?.memory_usage ?? 0}
              suffix="%"
              precision={1}
            />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <Statistic
              title="Disk Usage"
              value={currentMetrics?.disk_usage ?? 0}
              suffix="%"
              precision={1}
            />
          </Card>
        </Col>
      </Row>

      <Card style={{ marginTop: "16px" }}>
        <LineChart
          width={800}
          height={400}
          data={metrics}
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="timestamp"
            tickFormatter={(value) => new Date(value).toLocaleTimeString()}
          />
          <YAxis />
          <Tooltip
            labelFormatter={(value) => new Date(value).toLocaleTimeString()}
          />
          <Legend />
          <Line
            type="monotone"
            dataKey="cpu_usage"
            stroke="#8884d8"
            name="CPU Usage"
          />
          <Line
            type="monotone"
            dataKey="memory_usage"
            stroke="#82ca9d"
            name="Memory Usage"
          />
          <Line
            type="monotone"
            dataKey="disk_usage"
            stroke="#ffc658"
            name="Disk Usage"
          />
        </LineChart>
      </Card>
    </div>
  );
};
