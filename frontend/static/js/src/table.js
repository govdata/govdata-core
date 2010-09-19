iv.Table = function(opts) {
    iv.Module.call(this,opts);
    _.extend(iv.Table.prototype.settings, opts.settings);
};

_.extend(iv.Table.prototype,iv.Module.prototype);

iv.Table.prototype.settings = {
    cellWidth: 100,
    cellHeight: 50
};

/**
 * Fetch and show the data for the given bounding box
 *
 **/
iv.Table.prototype.fetch = function(x,y,width,height) {
    // calc which records need to be fetched
    this.collection.fetch()
}

iv.Table.prototype.dataTemplate = _.template("\
<table width=<%= width %>>\
<% _.each(data, function(v) { %>\
  <tr>\
  <% _.each(v, function(v,k) { %>\
        <td><%= k %> : <%= v %></td>\
  <% }); %>\
  </tr>\
<% }); %> \
</table>\
");

iv.Table.prototype.headerTemplate = _.template("\
<table width=<%= width %>><tr>\
<% _.each(cols, function(c) { %>\
    <td><%= c %></td>\
<% }); %>\
</tr></table>\
");

iv.Table.prototype.template = _.template("\
<div id='tableContainer'>\
<div id='tableSpacer' style='width:<%= width*1.5 %>px; height:<%= height+this.settings.cellHeight*1.5 %>px;'>\
</div>\
<div id='tableData'>\
<%= this.dataTemplate({data : data, settings : this.settings, width: width, height: height}) %> \
</div>\
<div id='tableHeader'>\
<%= this.headerTemplate({cols : cols, settings : this.settings, width: width, height: height}) %> \
</div>\
</div>\
");

/**
 *  data is an array of objects columnname to value
 * [{'row1colName1' : 'value1', 'row1colName2' : 'value2'}, ... , {'rowNcolName1', 'value1'}]
 */ 
iv.Table.prototype.view = function(data) {
    var cols = _(data).chain().first().map(function(v,k){return k;}).value();
    console.log(this.metadata.numCols);
    data = { name : "table", data : data, cols : cols, 
        width: this.settings.cellWidth*this.metadata.numCols, 
        height: this.settings.cellHeight*this.metadata.numRows };
    return this.template(data);
};

iv.Table.prototype.update = function() {
    var table = $("#table");
    var sTop = table.scrollTop();
    var sLeft = table.scrollLeft();
    var scrollbar = 0;
    if (this.scrollbar) {
        scrollbar = this.scrollbar;
    } else {
        scrollbar = this.scrollbar = (table.innerWidth()-table.width())/2.0;
    }
    // LOTS OF BROWSER SPECIFIC HACKS NEEDED HERE :(
    sTop = Math.max(0,sTop-scrollbar);
    sLeft = Math.max(0,sLeft-scrollbar);
    $("#tableHeader").css({top: sTop},100);
    $("#tableData").css({left: sLeft},100);
    console.log(sLeft);
}

iv.Table.prototype.render = function() {
    this.container.innerHTML = this.view(Show.example);
    var esta = this;
    $("#table").scroll(function() {
        clearTimeout(esta.updateTimer); 
        esta.updateTimer = setTimeout(esta.update, 100);
    })
    // $("#tableHeader, #tableData").css({width: 10000});
    // $("#tableData .cell, #tableHeader li").css({width: 100, height: 100});
    // $("#tableData").css({left : 100, right: 100});
}



