import { render } from "preact";
import "./styles/tokens.css";
import "./router";
import "./theme";
import { App } from "./app";
import { loadAll } from "./store/data";

render(<App />, document.getElementById("app")!);
void loadAll();
