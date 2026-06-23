// dibuja el grafico de barras con d3 y la tabla, leyendo del endpoint /api.
// refresca solito cada cierto tiempo para reflejar datos nuevos sin recargar :)

(function () {
  var contenedor = document.getElementById("vista-analisis");
  if (!contenedor) return;

  var nombre = contenedor.dataset.nombre;
  var barra = contenedor.dataset.barra ? JSON.parse(contenedor.dataset.barra) : null;
  var modoColor = contenedor.dataset.color || "magnitud";

  var INTERVALO_MS = 30000;
  var tooltip = crearTooltip();

  // leemos el acento real desde el css para no repetir el color a mano
  var ACENTO = getComputedStyle(document.documentElement).getPropertyValue("--acento").trim() || "#d6361f";
  var GRIS = "#4a4a4f";
  var VERDE = leerVar("--dato-bajo", "#5fb07a");
  var AMBAR = leerVar("--dato-medio", "#d9a441");
  var ROJO = leerVar("--dato-alto", "#d6543a");

  function leerVar(nombreVar, fallback) {
    var v = getComputedStyle(document.documentElement).getPropertyValue(nombreVar).trim();
    return v || fallback;
  }

  function crearTooltip() {
    var t = document.createElement("div");
    t.className = "tooltip";
    document.body.appendChild(t);
    return t;
  }

  function aNumero(valor) {
    var n = parseFloat(valor);
    return isNaN(n) ? null : n;
  }

  function colorDe(valor, maxAbs) {
    if (modoColor === "signo") {
      if (valor > 0) return VERDE;
      if (valor < 0) return ROJO;
      return AMBAR;
    }
    // magnitud: rampa de gris a acento segun intensidad
    var t = maxAbs ? Math.abs(valor) / maxAbs : 0;
    return d3.interpolateRgb(GRIS, ACENTO)(0.3 + 0.7 * t);
  }

  function filasParaGrafico(datos) {
    if (!barra) return [];
    // agrupamos por etiqueta y sumamos, asi la misma etiqueta (ej. un pais que
    // aparece en varios dias) no genera barras encimadas. para los analisis que
    // ya traen una fila por etiqueta la suma es de un solo valor, no afecta
    var acumulado = {};
    datos.documentos.forEach(function (doc) {
      var valor = aNumero(doc[barra.valor]);
      if (valor === null) return;
      var etiqueta = barra.label
        .map(function (campo) { return doc[campo] !== undefined ? doc[campo] : ""; })
        .join(" - ");
      acumulado[etiqueta] = (acumulado[etiqueta] || 0) + valor;
    });
    var filas = Object.keys(acumulado).map(function (et) {
      return { etiqueta: et, valor: acumulado[et] };
    });
    filas.sort(function (a, b) { return Math.abs(b.valor) - Math.abs(a.valor); });
    return filas.slice(0, barra.top || 20);
  }

  function dibujarGrafico(datos) {
    var filas = filasParaGrafico(datos);
    var host = d3.select("#grafico");
    if (!filas.length) {
      host.html("<p class='vacio'>este analisis no tiene una metrica para graficar, vea la tabla abajo</p>");
      return;
    }
    // quitamos el texto de carga la primera vez, el svg se queda para las animaciones
    host.selectAll("p").remove();

    var margen = { top: 4, right: 70, bottom: 4, left: 230 };
    var alturaFila = 22;
    var ancho = host.node().clientWidth || 760;
    var alto = filas.length * alturaFila + margen.top + margen.bottom;

    var maxAbs = d3.max(filas, function (d) { return Math.abs(d.valor); }) || 1;

    var x = d3.scaleLinear().domain([0, maxAbs]).range([0, ancho - margen.left - margen.right]);
    var y = d3.scaleBand()
      .domain(filas.map(function (d) { return d.etiqueta; }))
      .range([0, filas.length * alturaFila])
      .padding(0.18);

    var svg = host.selectAll("svg").data([null]);
    svg = svg.enter()
      .append("svg")
      .attr("class", "grafico-svg")
      .merge(svg)
      .attr("width", ancho)
      .attr("height", alto);

    var g = svg.selectAll("g.lienzo").data([null]);
    g = g.enter().append("g").attr("class", "lienzo").merge(g)
      .attr("transform", "translate(" + margen.left + "," + margen.top + ")");

    var grupos = g.selectAll("g.barra").data(filas, function (d) { return d.etiqueta; });
    grupos.exit().remove();

    var entrada = grupos.enter().append("g").attr("class", "barra");
    entrada.append("rect").attr("width", 0);
    entrada.append("text").attr("class", "et");
    entrada.append("text").attr("class", "val");

    var todos = entrada.merge(grupos);

    todos.select("rect")
      .attr("x", 0)
      .attr("y", function (d) { return y(d.etiqueta); })
      .attr("height", y.bandwidth())
      .attr("fill", function (d) { return colorDe(d.valor, maxAbs); })
      .transition().duration(500)
      .attr("width", function (d) { return Math.max(1, x(Math.abs(d.valor))); });

    todos.select("text.et")
      .attr("x", -10)
      .attr("y", function (d) { return y(d.etiqueta) + y.bandwidth() / 2 + 4; })
      .attr("text-anchor", "end")
      .text(function (d) { return recortar(d.etiqueta, 34); });

    todos.select("text.val")
      .attr("y", function (d) { return y(d.etiqueta) + y.bandwidth() / 2 + 4; })
      .attr("x", function (d) { return x(Math.abs(d.valor)) + 6; })
      .attr("text-anchor", "start")
      .attr("fill", "#ededec")
      .text(function (d) { return formato(d.valor); });

    todos
      .on("mousemove", function (evento, d) {
        tooltip.style.opacity = "1";
        tooltip.style.left = (evento.clientX + 14) + "px";
        tooltip.style.top = (evento.clientY + 14) + "px";
        tooltip.innerHTML = "<strong>" + escapar(d.etiqueta) + "</strong><br>" +
          escapar(barra.valor) + ": " + formato(d.valor);
      })
      .on("mouseleave", function () { tooltip.style.opacity = "0"; });
  }

  function dibujarTabla(datos) {
    var host = d3.select("#tabla");
    if (!datos.documentos.length) {
      host.html("<p class='vacio'>esta coleccion no tiene datos por ahora :(</p>");
      return;
    }
    var html = "<div class='tabla-scroll'><table><thead><tr>";
    datos.columnas.forEach(function (col) { html += "<th>" + escapar(col) + "</th>"; });
    html += "</tr></thead><tbody>";
    datos.documentos.forEach(function (doc) {
      html += "<tr>";
      datos.columnas.forEach(function (col) {
        var valor = doc[col] !== undefined && doc[col] !== null ? doc[col] : "";
        var clase = aNumero(valor) !== null ? " class='numerico'" : "";
        html += "<td" + clase + ">" + escapar(String(valor)) + "</td>";
      });
      html += "</tr>";
    });
    html += "</tbody></table></div>";
    host.html(html);
  }

  function actualizarMeta(datos) {
    var meta = document.getElementById("meta");
    if (meta) {
      meta.textContent = "filas unicas: " + datos.mostradas +
        " (de " + datos.total + " registros con repeticiones del pipeline)" +
        " | actualizado " + new Date().toLocaleTimeString();
    }
  }

  function recortar(texto, n) {
    return texto.length > n ? texto.slice(0, n - 1) + "." : texto;
  }

  function formato(n) {
    if (Math.abs(n) >= 1000) return d3.format(",")(Math.round(n));
    return d3.format(".4~f")(n);
  }

  function escapar(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }

  function cargar() {
    fetch("/api/analisis/" + encodeURIComponent(nombre))
      .then(function (r) { return r.json(); })
      .then(function (datos) {
        actualizarMeta(datos);
        dibujarGrafico(datos);
        dibujarTabla(datos);
      })
      .catch(function () {
        var meta = document.getElementById("meta");
        if (meta) meta.textContent = "no se pudo cargar los datos, reintentando.";
      });
  }

  cargar();
  setInterval(cargar, INTERVALO_MS);
  window.addEventListener("resize", function () {
    // redibujamos solo el grafico para que se ajuste al ancho nuevo
    fetch("/api/analisis/" + encodeURIComponent(nombre))
      .then(function (r) { return r.json(); })
      .then(dibujarGrafico)
      .catch(function () {});
  });
})();
