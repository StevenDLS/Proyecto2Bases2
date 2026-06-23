// refresca en vivo los numeros y las tarjetas de la pagina de inicio, pidiendo
// el resumen al backend cada cierto tiempo. asi el inicio se actualiza igual que
// las paginas de cada analisis :)

(function () {
  var rejilla = document.getElementById("rejilla");
  if (!rejilla) return;

  var INTERVALO_MS = 30000;
  var ultimoBueno = null;

  function escapar(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }

  function conComas(n) {
    return Number(n).toLocaleString("en-US");
  }

  function tarjeta(item, indice) {
    var extra = item.tiene_grafico ? " / gráfico" : "";
    return "" +
      "<article class='tarjeta'>" +
      "<div class='indice'>" + String(indice).padStart(2, "0") + "</div>" +
      "<div class='titulo'>" + escapar(item.titulo) + "</div>" +
      "<div class='pie'>" +
      "<span>" + conComas(item.filas) + " filas" + extra + "</span>" +
      "<a class='abrir' href='/analisis/" + encodeURIComponent(item.nombre) + "'>ver -&gt;</a>" +
      "</div>" +
      "</article>";
  }

  function pintar(datos) {
    var numAnalisis = document.getElementById("num-analisis");
    var numFilas = document.getElementById("num-filas");
    var numBruto = document.getElementById("num-bruto");
    if (numAnalisis) numAnalisis.textContent = datos.total_analisis;
    if (numFilas) numFilas.textContent = conComas(datos.total_filas);
    if (numBruto) numBruto.textContent = conComas(datos.total_bruto);

    if (!datos.analisis || !datos.analisis.length) {
      rejilla.innerHTML = "<p class='vacio'>todavía no hay datos en la base, corra el pipeline primero :/</p>";
      return;
    }
    var html = "";
    datos.analisis.forEach(function (item, i) {
      html += tarjeta(item, i + 1);
    });
    rejilla.innerHTML = html;
  }

  function cargar() {
    fetch("/api/resumen")
      .then(function (r) {
        if (!r.ok) throw new Error("http " + r.status);
        return r.json();
      })
      .then(function (datos) {
        // si llega vacio pero antes habia datos, es una ventana de recarga del
        // pipeline, conservamos lo ultimo bueno
        if ((!datos.analisis || !datos.analisis.length) && ultimoBueno) {
          return;
        }
        ultimoBueno = datos;
        pintar(datos);
      })
      .catch(function () {});
  }

  // el servidor ya pinto el estado inicial, asi que solo arrancamos el ciclo
  setInterval(cargar, INTERVALO_MS);
})();
