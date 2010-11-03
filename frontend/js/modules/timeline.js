iv.Timeline = function(opts) {
    iv.Module.call(this,opts);
    var timeline = this;
};

_.extend(iv.Timeline.prototype,iv.Module.prototype);

iv.Timeline.prototype.update = function() {
    if (this.data === undefined) {
        return;
    }
    $(this.container).empty().css("width","100%");
    var start = this.data[0].x;
    var end = this.data[this.data.length-1].x;
    /* Sizing and scales. */
    var w = $(this.container).width() - 20,
    h = w/3.0,
    x = pv.Scale.linear(start, end).range(0, w),
    y = pv.Scale.linear(0, pv.max(this.data, function(d){ return d.y; })).range(0, h);

    /* The root panel. */
    var vis = new pv.Panel()
      .canvas(this.container)
      .width(w)
      .height(h)
      .bottom(20)
      .left(20)
      .right(10)
      .top(5);

    /* X-axis ticks. */
    vis.add(pv.Rule)
      .data(x.ticks())
      .visible(function(d){ return d > 0; })
      .left(x)
      .strokeStyle("#eee")
    .add(pv.Rule)
      .bottom(-5)
      .height(5)
      .strokeStyle("#000")
    .anchor("bottom").add(pv.Label)
      .text(x.tickFormat);

    /* Y-axis ticks. */
    vis.add(pv.Rule)
      .data(y.ticks(5))
      .bottom(y)
      .strokeStyle(function(d){ return (d ? "#eee" : "#000");})
    .anchor("left").add(pv.Label)
      .text(y.tickFormat);

    /* The line. */
    vis.add(pv.Line)
      .data(this.data)
      //.interpolate("step-after")
      .left(function(d){ return x(d.x);})
      .bottom(function(d){ return y(d.y);})
      .lineWidth(3);

    this.renderer = vis;
};

iv.Timeline.prototype.render = function() {
    if(this.renderer) {
        this.renderer.render();
    }
};

iv.Timeline.prototype.add = function(data) {
    console.log(data);
    this.data = data;
    this.update();
    this.render();
}
