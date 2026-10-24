// Login page: carry ?next= through the form, and say why the last try failed.
(function () {
  "use strict";
  var KV = window.KV;
  var MESSAGES = {
    invalid: "That username and password don't match.",
    locked: "Too many failed attempts from this address. Try again in a few minutes.",
  };

  document.addEventListener("DOMContentLoaded", function () {
    var params = new URLSearchParams(window.location.search);
    var next = params.get("next");
    if (next && next.charAt(0) === "/" && next.slice(0, 2) !== "//") KV.qs("next").value = next;
    var USER_KEY = "kividb-gui-login-user";
    var err = MESSAGES[params.get("error")];
    try {
      if (err && sessionStorage.getItem(USER_KEY)) KV.qs("username").value = sessionStorage.getItem(USER_KEY);
    } catch (e) {}
    if (err) {
      var box = KV.qs("login-error");
      box.textContent = err;
      box.classList.remove("hidden");
      KV.qs("password").focus();
    }
    KV.qs("login-form").addEventListener("submit", function () {
      try {
        sessionStorage.setItem(USER_KEY, KV.qs("username").value);
      } catch (e) {}
      var b = KV.qs("login-submit");
      b.disabled = true;
      b.textContent = "Signing in…";
    });
  });
})();
