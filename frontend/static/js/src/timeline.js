iv.Timeline = function(opts) {
    iv.Module.call(this,opts);
    console.log("WTF");
    console.log(this.container);
    console.log("WTF!");
    this.renderer = this.generator();
    console.log(this.renderer);
}
_.extend(iv.Timeline.prototype,iv.Module.prototype);

iv.Timeline.prototype.generator = function() {
    
    var start = new Date(1990, 0, 1);
      var year = 1000 * 60 * 60 * 24 * 365;
      var data = pv.range(0, 20, .02).map(function(x) {
          return {x: new Date(start.getTime() + year * x),
                  y: (1 + .1 * (Math.sin(x * 2 * Math.PI))
                      + Math.random() * .1) * Math.pow(1.18, x)
                      + Math.random() * .1};
        });
      var end = data[data.length - 1].x;
      
      var w = 720,
          h1 = 300,
          h2 = 30,
          x = pv.Scale.linear(start, end).range(0, w),
          y = pv.Scale.linear(0, pv.max(data, function(d){ return d.y; })).range(0, h2);
     
      /* Interaction state. Focus scales will have domain set on-render. */
      var i = {x:200, dx:100},
          fx = pv.Scale.linear().range(0, w),
          fy = pv.Scale.linear().range(0, h1);
     
      /* Root panel. */
      var vis = new pv.Panel().canvas(this.container)
          .width(w)
          .height(h1 + 20 + h2)
          .bottom(20)
          .left(30)
          .right(20)
          .top(5);
     
      /* Focus panel (zoomed in). */
      var focus = vis.add(pv.Panel)
          .def("init", function() {
              var d1 = x.invert(i.x),
                  d2 = x.invert(i.x + i.dx),
                  dd = data.slice(
                      Math.max(0, pv.search.index(data, d1, function(d){ return d.x; }) - 1),
                      pv.search.index(data, d2, function(d){ return d.x; }) + 1);
              fx.domain(d1, d2);
              fy.domain([0, pv.max(dd, function(d){ return d.y; })]);
              // fy.domain(scale.checked ? [0, pv.max(dd, function(d){ return d.y; })] : y.domain());
              return dd;
            })
          .top(0)
          .height(h1);
           
      /* X-axis ticks. */
      focus.add(pv.Rule)
          .data(function(){ return fx.ticks();})
          .left(fx)
          .strokeStyle("#eee")
        .anchor("bottom").add(pv.Label)
          .text(fx.tickFormat);
           
      /* Y-axis ticks. */
      focus.add(pv.Rule)
          .data(function(){ return fy.ticks(7);})
          .bottom(fy)
          .strokeStyle(function(d){ return (d ? "#aaa" : "#000"); })
        .anchor("left").add(pv.Label)
          .text(fy.tickFormat);
           
      /* Focus area chart. */
      focus.add(pv.Panel)
          .overflow("hidden")
        .add(pv.Line)
          .data(function(){ return focus.init(); })
          .left(function(d){ return fx(d.x); })
          .bottom(1)
          .height(function(d){ return fy(d.y);})
          .fillStyle("lightsteelblue")
        .anchor("top").add(pv.Line)
          .fillStyle(null)
          .strokeStyle("steelblue")
          .lineWidth(3);
           
      /* Context panel (zoomed out). */
      var context = vis.add(pv.Panel)
          .bottom(0)
          .height(h2);
           
      /* X-axis ticks. */
      context.add(pv.Rule)
          .data(x.ticks())
          .left(x)
          .strokeStyle("#eee")
        .anchor("bottom").add(pv.Label)
          .text(x.tickFormat);
           
      /* Y-axis ticks. */
      context.add(pv.Rule)
          .bottom(0);
           
      /* Context area chart. */
      context.add(pv.Line)
          .data(data)
          .left(function(d){ return x(d.x);})
          .bottom(1)
          .height(function(d){ return y(d.y);})
          .fillStyle("lightsteelblue")
        .anchor("top").add(pv.Line)
          .strokeStyle("steelblue")
          .lineWidth(2);
           
      /* The selectable, draggable focus region. */
      context.add(pv.Panel)
          .data([i])
          .cursor("crosshair")
          .events("all")
          .event("mousedown", pv.Behavior.select())
          .event("select", focus)
        .add(pv.Bar)
          .left(function(d){ return d.x;})
          .width(function(d){ return d.dx;})
          .fillStyle("rgba(255, 128, 128, .4)")
          .cursor("move")
          .event("mousedown", pv.Behavior.drag())
          .event("drag", focus);
      return vis;
}

iv.Timeline.prototype.render = function() {
    this.renderer.render();
    console.log(document.getElementById('map'));
}