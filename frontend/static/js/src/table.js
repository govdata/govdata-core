iv.Table = function(opts) {
    iv.Module.call(this,opts);
    _.extend(iv.Table.prototype.settings, opts.settings);
    
    // Extra settings calculations
    
    var esta = this;
    $(this.container).scroll(function() {
        clearTimeout(esta.updateTimer); 
        esta.updateTimer = setTimeout(function(){ esta.update(esta); }, 100);
    });
};

_.extend(iv.Table.prototype,iv.Module.prototype);

iv.Table.prototype.settings = {
    cellWidth: 100,
    cellHeight: 50,
    bufferWidth: 100, // in number of cells
    bufferHeight: 100 // in number of rows
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
<table width=<%= settings.cellWidth*metadata.numCols %>>\
<% _.each(data, function(row) { %>\
  <tr>\
  <% _.each(metadata.columns, function(c,i) { %>\
      <td><div>\
      <% var unprocessed = true; %>\
       <%  _.each(metadata.valueProcessors, function(fn, vp) { %>\
          <%  if(_.include(metadata.columnGroups[vp],c)) { %>\
                  <%= fn(row[i]) %>\
                  <% unprocessed = false; %>\
          <%  } %>\
      <%  }); %>\
  <%  if (unprocessed) { %>\
          <%= row[i] %>\
      <%  } %>\
      </div></td>\
  <% }); %>\
  </tr>\
<% }); %> \
</table>\
");

iv.Table.prototype.headerTemplate = _.template("\
<table width=<%= settings.cellWidth*metadata.numCols %>><tr>\
<% _.each(metadata.columns, function(c) { %>\
        <td><div>\
        <% var unprocessed = true; %>\
         <%  _.each(metadata.nameProcessors, function(fn, np) { %>\
            <%  if(_.include(metadata.columnGroups[np],c)) { %>\
                    <%= fn(c) %>\
                    <% unprocessed = false; %>\
            <%  } %>\
        <%  }); %>\
    <%  if (unprocessed) { %>\
            <%= c %>\
        <%  } %>\
        </div></td>\
<% }); %>\
</tr></table>\
");

iv.Table.prototype.template = _.template("\
<div id='tableContainer'>\
<div id='tableSpacer' style='width: <%= this.settings.cellWidth*this.metadata.numCols %>px; height: <%= this.metadata.numRows+this.settings.cellHeight %>px;'>\
</div>\
<div id='tableData'>\
<%= this.dataTemplate({data : data, metadata: this.metadata, settings : this.settings }) %> \
</div>\
<div id='tableHeader'>\
<%= this.headerTemplate({metadata : this.metadata, settings : this.settings }) %> \
</div>\
</div>\
");

/**
 *  data is an array of objects columnname to value
 * [{'row1colName1' : 'value1', 'row1colName2' : 'value2'}, ... , {'rowNcolName1', 'value1'}]
 */ 
iv.Table.prototype.view = function(data) {
    var cols = this.metadata.columns
    data = { data : data }
    return this.template(data);
};

iv.Table.prototype.update = function(self) {
    var table = $("#table");
    var sTop = table.scrollTop();
    var sLeft = table.scrollLeft();
    var scrollbar = 0;
    if (self.scrollbar) {
        scrollbar = self.scrollbar;
    } else {
        scrollbar = self.scrollbar = (table.innerWidth()-table.width())/2.0;
    }
    // LOTS OF BROWSER SPECIFIC HACKS NEEDED HERE :(
    sTop = Math.max(0,sTop-scrollbar);
    sLeft = Math.max(0,sLeft-scrollbar);
    $("#tableHeader").css({top: sTop},100);
    // $("#tableData").css({left: sLeft},100);
    console.log(sLeft);
    
    var rerender = function() {
        $("#tableData").html(self.dataTemplate({data : Show.data, metadata: self.metadata, settings: self.settings}))
    }
    clearTimeout(this.rerenderTimer); 
    this.rerenderTimer = setTimeout(rerender, 100);    
}

iv.Table.prototype.render = function(callback) {
    var esta = this;
    this.collection.fetch({},function(data){
        Show.data = data;
        esta.container.innerHTML = esta.view(data);
        if (callback) callback();
        console.log(data);
    });
}



