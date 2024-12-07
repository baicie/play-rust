import React from "react";
import { Menu } from "antd";
import { DashboardOutlined, PlusOutlined } from "@ant-design/icons";
import { useNavigate } from "react-router-dom";

const Sidebar: React.FC = () => {
  const navigate = useNavigate();

  return (
    <Menu
      theme="dark"
      mode="inline"
      defaultSelectedKeys={["dashboard"]}
      onClick={({ key }) => navigate(key)}
    >
      <Menu.Item key="/" icon={<DashboardOutlined />}>
        Dashboard
      </Menu.Item>
      <Menu.Item key="/create-job" icon={<PlusOutlined />}>
        Create Job
      </Menu.Item>
    </Menu>
  );
};

export default Sidebar;
