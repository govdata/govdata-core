iv.Table = function(opts) {
    iv.Module.call(this,opts);
    _.extend(this,opts);

    var esta = this;
    console.log(this.container);
    $(this.container).html(esta.template({
        metadata : esta.metadata
    }));
    $(this.container).find("table").dataTable( {
        //bScrollInfinite : true,
        //bScrollCollapse : true,
        sPaginationType : "full_numbers",
        sScrollY : "300px",
        sScrollX : "760px",
        sAjaxSource : '',
        bSort : false,
        bFilter : true,
        iDisplayLength : 100,
        bProcessing : true,
        bJQueryUI : true,
        bAutoWidth : false,
        bLengthChange : false,
        bServerSide : true,
        aoColumnDefs : [{
            aTargets : [0,1,2,3],
            bVisible : false
        },{
            aTargets : ["_all"],
            fnRender : function(o) {
                var val = o.aData[o.iDataColumn];
                if(val === undefined){
                    return "<div class='undefined'></div>"
                } else {
                    return val;
                }
            }
        }],
        fnServerData: function ( sSource, aoData, fnCallback ) {
            console.log(esta.metadata.volume);
            esta.serverData({
                limit : aoData.iDisplayLength,
                skip : aoData.iDisplayStart
            }, function(data) {
                data = esta.transformer(data,esta.metadata)
                fnCallback({
                    iTotalRecords : esta.metadata.volume,
                    iTotalDisplayRecords: esta.metadata.volume,
                    sEcho : parseInt(aoData.sEcho),
                    aaData : data
                });
            });
        }
    } );
};

_.extend(iv.Table.prototype,iv.Module.prototype);


// <% if (_.include(metdata.))

iv.Table.prototype.template = _.template("\
<table>\
<thead>\
<% _.each(metadata.showCols, function(col,i) { %>\
    <th><%= col %></th>\
<% }); %>\
</thead>\
<tbody>\
<tbody>\
</table>\
");

iv.Table.prototype.render = _.identity;


