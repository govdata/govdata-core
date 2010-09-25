iv.Table = function(opts) {
    iv.Module.call(this,opts);
    this.metadata = opts.metadata;
    this.transformer = opts.transformer;
    this.serverData = opts.serverData;

    _.extend(iv.Table.prototype.settings, opts.settings);
    var esta = this;
    console.log(this.container);
    $(this.container).html(esta.template({
        metadata : esta.metadata
    }))
    $(this.container).find("table").dataTable( {
        bScrollInfinite : true,
        bScrollCollapse : true,
        sScrollY : "200px",
        sScrollX : "760px",
        sAjaxSource : '',
        bSort : false,
        bProcessing : true,
        bJQueryUI : true,
        iDisplayLength: 10,
        fnServerData: function ( sSource, aoData, fnCallback ) {
            console.log(aoData);
            esta.serverData({
                limit : aoData.iDisplayLength,
                skip : aoData.iDisplayStart
            }, function(data) {
                data = esta.transformer(data,esta.metadata)
                fnCallback({
                    iTotalRecords : esta.metadata.numRows,
                    iTotalDisplayRecords: esta.metadata.numRows,
                    sEcho : aoData.sEcho,
                    aaData : data
                });
            });
        }
    } );
};

_.extend(iv.Table.prototype,iv.Module.prototype);

iv.Table.prototype.settings = {
};

iv.Table.prototype.template = _.template("\
<table width=<%= metadata.columns.length*100 %>px >\
<thead>\
<% _.each(metadata.columns, function(col,i) { %>\
    <th width=100px><%= col %></th>\
<% }); %>\
</thead>\
<tbody>\
<tbody>\
</table>\
");

iv.Table.prototype.render = _.identity;


