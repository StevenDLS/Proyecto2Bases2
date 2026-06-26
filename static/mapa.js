// mapa del mundo de intensidad de conflictos. dibuja el contorno de los paises con
// d3-geo (offline, sin tiles) y encima un circulo por pais: tamaño segun la cantidad
// de eventos de conflicto y color segun la intensidad goldstein promedio ^.^

(function () {
  var cont = document.getElementById("mapa");
  var vista = document.getElementById("vista-analisis");
  if (!cont || !vista || typeof topojson === "undefined") return;

  var nombre = vista.dataset.nombre;
  var INTERVALO_MS = 30000;

  var VERDE = leerVar("--dato-bajo", "#5fb07a");
  var AMBAR = leerVar("--dato-medio", "#d9a441");
  var ROJO = leerVar("--dato-alto", "#d6543a");

  var tooltip = crearTooltip();
  var mundo = null;
  var ultimoDatos = null;

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

  function escapar(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }

  function agregarPorPais(documentos) {
    // un pais aparece en varias fechas, lo juntamos: sumamos eventos, promediamos
    // intensidad y lat/lon
    var mapa = {};
    documentos.forEach(function (doc) {
      var pais = doc.pais;
      var lat = Number(doc.latitud);
      var lon = Number(doc.longitud);
      if (!pais || isNaN(lat) || isNaN(lon)) return;
      var eventos = Number(doc.total_eventos_conflicto) || 0;
      var inten = Number(doc.intensidad_promedio_goldstein);
      if (!mapa[pais]) {
        mapa[pais] = { pais: pais, eventos: 0, latSum: 0, lonSum: 0, n: 0, intenSum: 0, intenN: 0 };
      }
      var o = mapa[pais];
      o.eventos += eventos;
      o.latSum += lat;
      o.lonSum += lon;
      o.n += 1;
      if (!isNaN(inten)) { o.intenSum += inten; o.intenN += 1; }
    });
    return Object.keys(mapa).map(function (k) {
      var o = mapa[k];
      return {
        pais: o.pais,
        eventos: o.eventos,
        lat: o.latSum / o.n,
        lon: o.lonSum / o.n,
        inten: o.intenN ? o.intenSum / o.intenN : 0,
      };
    });
  }

  function pintar() {
    if (!mundo || !ultimoDatos) return;
    var ancho = cont.clientWidth || 760;
    var alto = Math.round(ancho * 0.52);

    var proj = d3.geoNaturalEarth1().fitSize([ancho, alto], mundo);
    var path = d3.geoPath(proj);

    // quitamos el texto de carga y redibujamos el svg (un mapa no necesita animar)
    d3.select(cont).selectAll("p").remove();
    d3.select(cont).selectAll("svg").remove();
    var svg = d3.select(cont).append("svg")
      .attr("class", "mapa-svg")
      .attr("width", ancho)
      .attr("height", alto);

    svg.append("g").selectAll("path")
      .data(mundo.features)
      .enter().append("path")
      .attr("class", "pais-geo")
      .attr("d", path);

    var datos = agregarPorPais(ultimoDatos.documentos);
    if (!datos.length) return;

    var maxEventos = d3.max(datos, function (d) { return d.eventos; }) || 1;
    var radio = d3.scaleSqrt().domain([0, maxEventos]).range([2, 22]);

    var intensidades = datos.map(function (d) { return d.inten; });
    var minI = d3.min(intensidades);
    var maxI = d3.max(intensidades);
    var color = d3.scaleLinear()
      .domain([minI, (minI + maxI) / 2, maxI])
      .range([VERDE, AMBAR, ROJO])
      .clamp(true);

    // ordenamos para que los circulos chicos queden encima de los grandes
    datos.sort(function (a, b) { return b.eventos - a.eventos; });

    svg.append("g").selectAll("circle")
      .data(datos)
      .enter().append("circle")
      .attr("class", "punto-mapa")
      .attr("transform", function (d) {
        var p = proj([d.lon, d.lat]);
        return p ? "translate(" + p[0] + "," + p[1] + ")" : "translate(-50,-50)";
      })
      .attr("r", function (d) { return radio(d.eventos); })
      .attr("fill", function (d) { return color(d.inten); })
      .on("mousemove", function (evento, d) {
        tooltip.style.opacity = "1";
        tooltip.style.left = (evento.clientX + 14) + "px";
        tooltip.style.top = (evento.clientY + 14) + "px";
        tooltip.innerHTML = "<strong>" + escapar(d.pais) + "</strong><br>" +
          "eventos de conflicto: " + d3.format(",")(Math.round(d.eventos)) + "<br>" +
          "intensidad goldstein: " + d3.format(".2f")(d.inten);
      })
      .on("mouseleave", function () { tooltip.style.opacity = "0"; });
  }

  function cargarDatos() {
    fetch("/api/analisis/" + encodeURIComponent(nombre))
      .then(function (r) {
        if (!r.ok) throw new Error("http " + r.status);
        return r.json();
      })
      .then(function (d) {
        if (d.documentos && d.documentos.length) ultimoDatos = d;
        pintar();
      })
      .catch(function () {});
  }

  // primero cargamos la geometria del mundo una sola vez, despues los datos
  fetch("/static/countries-110m.json")
    .then(function (r) { return r.json(); })
    .then(function (topo) {
      mundo = topojson.feature(topo, topo.objects.countries);
      cargarDatos();
      setInterval(cargarDatos, INTERVALO_MS);
      window.addEventListener("resize", pintar);
    })
    .catch(function () {
      cont.innerHTML = "<p class='vacio'>no se pudo cargar el mapa :/</p>";
    });
})();
