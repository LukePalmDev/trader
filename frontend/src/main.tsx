import { render } from "preact";
import "./styles/tokens.css";
import "./router";
import "./theme";
import { App } from "./app";

render(<App />, document.getElementById("app")!);
