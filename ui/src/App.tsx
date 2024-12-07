import { RouterProvider } from "react-router-dom";
import { router } from "./router";
import { LoadingScreen } from "./components/LoadingScreen";

function App() {
  return (
    <>
      <LoadingScreen />
      <RouterProvider router={router} />
    </>
  );
}

export default App;
