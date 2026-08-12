(() => {
  "use strict";

  const terminal = document.querySelector("#terminal");
  const output = document.querySelector("#output");
  const prompt = document.querySelector("#prompt");
  const promptLabel = document.querySelector("#prompt-label");
  const commandInput = document.querySelector("#command");
  const login = document.querySelector("#login");
  const loginForm = document.querySelector("#login-form");
  const tokenInput = document.querySelector("#setup-token");
  const loginError = document.querySelector("#login-error");
  const connectionDot = document.querySelector("#connection-dot");
  const connectionLabel = document.querySelector("#connection-label");

  const commands = [
    "help", "clear", "version", "lsblk", "disk inspect", "disk format", "disk mount",
    "disk assign", "disk unassign", "cluster init", "cluster show", "cluster validate", "cluster commit",
    "cluster evolve", "cluster discard-draft", "cluster start", "cluster stop", "cluster restart",
    "cluster enable", "cluster disable", "cluster status",
    "pool create", "pool enroll", "pool seal", "pool writable", "reserve enroll", "reserve remove",
    "node pair", "node list", "config tls", "gateway add", "gateway remove", "logs"
  ];
  let socket = null;
  let hostname = "localhost";
  let promptState = "disconnected";
  let history = loadHistory();
  let historyIndex = history.length;
  let pendingCommand = null;
  let busy = false;
  let reconnectTimer = null;
  let activeToken = "";

  function loadHistory() {
    try {
      const value = JSON.parse(localStorage.getItem("n2-shell-history") || "[]");
      return Array.isArray(value) ? value.filter(item => typeof item === "string").slice(-200) : [];
    } catch (_) {
      return [];
    }
  }

  function saveHistory() {
    localStorage.setItem("n2-shell-history", JSON.stringify(history.slice(-200)));
  }

  function addLine(text, kind = "normal") {
    if (text === "") {
      const line = document.createElement("div");
      line.className = `line ${kind}`;
      line.textContent = " ";
      output.append(line);
    } else {
      for (const value of String(text).split("\n")) {
        const line = document.createElement("div");
        line.className = `line ${kind}`;
        line.textContent = value || " ";
        output.append(line);
      }
    }
    requestAnimationFrame(() => { terminal.scrollTop = terminal.scrollHeight; });
  }

  function updatePrompt() {
    promptLabel.textContent = `administrator@${hostname}:${promptState}$`;
  }

  function setConnection(state, label) {
    connectionDot.className = `dot ${state}`;
    connectionLabel.textContent = label;
  }

  function connect(token) {
    activeToken = token;
    setConnection("connecting", "Connecting");
    loginError.textContent = "";
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    socket = new WebSocket(`${protocol}//${location.host}/ws?token=${encodeURIComponent(token)}`);

    socket.addEventListener("open", () => {
      login.classList.add("hidden");
      setConnection("online", "Connected");
      commandInput.disabled = false;
      commandInput.focus();
    });

    socket.addEventListener("message", event => {
      let message;
      try { message = JSON.parse(event.data); } catch (_) { return; }
      if (message.type === "hello") {
        hostname = message.hostname || "localhost";
        promptState = message.prompt_state || "unconfigured";
        updatePrompt();
        addLine(`N2 Shell ${message.version} — guarded cluster commissioning`, "info");
        addLine("Type 'help' to list available commands.", "muted");
        addLine("", "normal");
      } else if (message.type === "output") {
        addLine(message.text, message.kind || "normal");
      } else if (message.type === "protocol_error") {
        addLine(message.message, "error");
        setBusy(false);
      } else if (message.type === "completed") {
        const outcome = message.outcome || {};
        promptState = outcome.prompt_state || promptState;
        updatePrompt();
        if (outcome.clear) output.replaceChildren();
        if (outcome.confirmation) {
          addLine(outcome.confirmation.prompt, "warning");
          pendingCommand = {
            line: pendingCommand?.line || "",
            expected: outcome.confirmation.expected
          };
          prompt.classList.add("confirming");
          promptLabel.textContent = ">";
          commandInput.type = "text";
          commandInput.disabled = false;
          commandInput.focus();
          busy = false;
        } else {
          pendingCommand = null;
          prompt.classList.remove("confirming");
          setBusy(false);
        }
      }
    });

    socket.addEventListener("close", event => {
      setConnection("offline", "Disconnected");
      commandInput.disabled = true;
      promptState = "disconnected";
      updatePrompt();
      if (!event.wasClean && login.classList.contains("hidden")) {
        addLine("Connection lost. Reconnecting does not repeat the previous command.", "warning");
        clearTimeout(reconnectTimer);
        reconnectTimer = setTimeout(() => connect(activeToken), 1500);
      } else if (!login.classList.contains("hidden")) {
        loginError.textContent = "Connection rejected. Verify the setup token.";
      }
    });

    socket.addEventListener("error", () => {
      if (!login.classList.contains("hidden")) loginError.textContent = "Could not connect to N2 Shell.";
    });
  }

  function setBusy(value) {
    busy = value;
    commandInput.disabled = value || !socket || socket.readyState !== WebSocket.OPEN;
    if (!value) {
      commandInput.value = "";
      updatePrompt();
      commandInput.focus();
    }
  }

  function submitCommand(line, confirmation = null) {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    setBusy(true);
    const id = globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`;
    socket.send(JSON.stringify({ id, line, confirmation }));
  }

  function commandForDisplay(line) {
    const parts = line.trim().split(/\s+/);
    if (parts[0] === "node" && parts[1] === "pair" && parts.length >= 5) {
      parts[4] = "<redacted-agent-token>";
      return parts.join(" ");
    }
    return line;
  }

  function commandIsSensitive(line) {
    return /^\s*node\s+pair\s+/i.test(line);
  }

  loginForm.addEventListener("submit", event => {
    event.preventDefault();
    const token = tokenInput.value.trim();
    if (token.length < 16) {
      loginError.textContent = "The setup token is incomplete.";
      return;
    }
    connect(token);
  });

  prompt.addEventListener("submit", event => {
    event.preventDefault();
    if (busy) return;
    const value = commandInput.value;
    if (value.includes("\n") || value.includes("\r")) {
      addLine("Multiline commands are not accepted.", "error");
      commandInput.value = "";
      return;
    }
    if (pendingCommand) {
      addLine(`> ${value}`, "command");
      const original = pendingCommand.line;
      pendingCommand = { ...pendingCommand, line: original };
      submitCommand(original, value);
      return;
    }
    const line = value.trim();
    commandInput.value = "";
    if (!line) return;
    addLine(`${promptLabel.textContent} ${commandForDisplay(line)}`, "command");
    if (!commandIsSensitive(line) && history.at(-1) !== line) history.push(line);
    historyIndex = history.length;
    saveHistory();
    pendingCommand = { line };
    submitCommand(line);
  });

  commandInput.addEventListener("keydown", event => {
    if (pendingCommand && event.key === "Escape") {
      event.preventDefault();
      pendingCommand = null;
      prompt.classList.remove("confirming");
      commandInput.value = "";
      updatePrompt();
      addLine("Confirmation cancelled.", "warning");
      return;
    }
    if (!pendingCommand && event.key === "ArrowUp") {
      event.preventDefault();
      if (historyIndex > 0) historyIndex -= 1;
      commandInput.value = history[historyIndex] || "";
      commandInput.setSelectionRange(commandInput.value.length, commandInput.value.length);
    } else if (!pendingCommand && event.key === "ArrowDown") {
      event.preventDefault();
      if (historyIndex < history.length) historyIndex += 1;
      commandInput.value = history[historyIndex] || "";
      commandInput.setSelectionRange(commandInput.value.length, commandInput.value.length);
    } else if (!pendingCommand && event.key === "Tab") {
      event.preventDefault();
      const value = commandInput.value.trimStart();
      const matches = commands.filter(command => command.startsWith(value));
      if (matches.length === 1) commandInput.value = `${matches[0]} `;
      else if (matches.length > 1) addLine(matches.join("    "), "muted");
    } else if (event.ctrlKey && event.key.toLowerCase() === "l") {
      event.preventDefault();
      output.replaceChildren();
    } else if (event.ctrlKey && event.key.toLowerCase() === "c") {
      event.preventDefault();
      commandInput.value = "";
      if (pendingCommand) {
        pendingCommand = null;
        prompt.classList.remove("confirming");
        updatePrompt();
        addLine("^C", "warning");
      }
    } else if (event.ctrlKey && event.key.toLowerCase() === "a") {
      event.preventDefault();
      commandInput.setSelectionRange(0, 0);
    } else if (event.ctrlKey && event.key.toLowerCase() === "e") {
      event.preventDefault();
      commandInput.setSelectionRange(commandInput.value.length, commandInput.value.length);
    }
  });

  terminal.addEventListener("pointerdown", event => {
    if (event.target === terminal || event.target === output) commandInput.focus({ preventScroll: true });
  });
})();
