import { Layout as AntLayout } from "antd";
import { Outlet } from "react-router-dom";
import Sidebar from "./Sidebar";

const { Content, Sider } = AntLayout;

export default function Layout() {
  return (
    <AntLayout style={{ minHeight: "100vh" }}>
      <Sider>
        <Sidebar />
      </Sider>
      <AntLayout>
        <Content style={{ padding: "24px" }}>
          <Outlet />
        </Content>
      </AntLayout>
    </AntLayout>
  );
}
