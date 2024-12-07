import React, { useState, useEffect } from "react";
import { Form, Select, Input, Button, Card } from "antd";
import { invoke } from "@tauri-apps/api/core";

interface ConnectorInfo {
  name: string;
  connector_type: string;
  schema: string;
}

export const JobCreator: React.FC = () => {
  const [connectors, setConnectors] = useState<ConnectorInfo[]>([]);
  const [form] = Form.useForm();

  useEffect(() => {
    invoke<ConnectorInfo[]>("get_available_connectors")
      .then(setConnectors)
      .catch(console.error);
  }, []);

  const handleSubmit = async (values: any) => {
    try {
      await invoke("create_sync_job", { config: values });
      // 显示成功消息
    } catch (error) {
      // 显示错误消息
      console.error(error);
    }
  };

  return (
    <Card title="Create Sync Job">
      <Form form={form} onFinish={handleSubmit} layout="vertical">
        <Form.Item
          label="Job Name"
          name="job_name"
          rules={[{ required: true }]}
        >
          <Input />
        </Form.Item>

        <Form.Item label="Source" required>
          <Form.Item
            name={["source", "connector_type"]}
            rules={[{ required: true }]}
          >
            <Select>
              {connectors
                .filter((c) => c.connector_type === "source")
                .map((c) => (
                  <Select.Option key={c.name} value={c.name}>
                    {c.name}
                  </Select.Option>
                ))}
            </Select>
          </Form.Item>

          <Form.Item
            name={["source", "properties"]}
            rules={[{ required: true }]}
          >
            <Input.TextArea
              rows={4}
              placeholder="Source configuration (JSON)"
            />
          </Form.Item>
        </Form.Item>

        <Form.Item label="Sink" required>
          <Form.Item
            name={["sink", "connector_type"]}
            rules={[{ required: true }]}
          >
            <Select>
              {connectors
                .filter((c) => c.connector_type === "sink")
                .map((c) => (
                  <Select.Option key={c.name} value={c.name}>
                    {c.name}
                  </Select.Option>
                ))}
            </Select>
          </Form.Item>

          <Form.Item name={["sink", "properties"]} rules={[{ required: true }]}>
            <Input.TextArea rows={4} placeholder="Sink configuration (JSON)" />
          </Form.Item>
        </Form.Item>

        <Form.Item>
          <Button type="primary" htmlType="submit">
            Create Job
          </Button>
        </Form.Item>
      </Form>
    </Card>
  );
};
