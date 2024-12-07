import { createBrowserRouter } from "react-router-dom";
import { Dashboard } from "../components/Dashboard";
import { JobCreator } from "../components/JobCreator";
import Layout from "../components/Layout";

export const router = createBrowserRouter([
  {
    path: "/",
    element: <Layout />,
    children: [
      {
        path: "/",
        element: <Dashboard />,
      },
      {
        path: "/create-job",
        element: <JobCreator />,
      },
    ],
  },
]);
