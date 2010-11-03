iv.Table = function(opts) {
    iv.Module.call(this,opts);
    _.extend(this,opts);

    var esta = this;
    var metadata = this.metadata;

    this.tableId = "table"+_.uniqueId();

    this.toShow = {};

    $(this.container).html(esta.template({
        metadata : metadata,
        id : esta.tableId
    }));

    var aoColumnDefs = _.map(metadata.showCols, function(col,i) {
        return {
            aTargets : [col],
            //sWidth: "100px",
            sName: col
        };
    });

    aoColumnDefs.push({
        aTargets : ["_all"],
        fnRender : function(o) {
            var val = o.aData[o.iDataColumn];
            var colindex = metadata.relativeToAbsolute[o.iDataColumn];
            var colName = metadata.showCols[colindex];
            if(val !== undefined) {
                if(!esta.toShow[colName]) {
                    esta.toShow[colName] = o.iDataColumn;
                }
            }
            if(metadata.isSpaceColumn(colName)) {
                return metadata.valueProcessors.spaceColumns(val);
            } else if(val === undefined){
                return "<div class='undefined'></div>";
            } else {
                return val;
            }
        }
    });

    var oTable = $("#"+this.tableId).dataTable( {
        //bScrollInfinite : true,
        //bScrollCollapse : true,
        sPaginationType : "full_numbers",
        sScrollY : "400px",
        //sScrollX : (190*6)+"px",
        sScrollX : "100%",
        //sScrollXInner : "150%",
        sAjaxSource : '',
        bSort : false,
        bFilter : false,
        iDisplayLength : 20,
        iDisplayStart : 0,
        bProcessing : true,
        bJQueryUI : true,
        bAutoWidth : true,
        bLengthChange : false,
        bServerSide : true,
        aoColumnDefs : aoColumnDefs,
        fnRowCallback : function(nRow, aData, iDisplayIndex) {
            $(nRow).data("idx",iDisplayIndex);
            return nRow;
        },
        fnDrawCallback : function() {
            //TODO: THIS TAKES A REALLY LONG TIME
            // because it has to remove and redraw the fnSetColumnVis call
            // should rewrite to unshow all at once then redraw
            // Or better check the data on load and remove the column
            var oTable = $("#"+esta.tableId).dataTable();
            //var cols = oTable.fnSettings().aoColumns;
            var toShow = _.keys(esta.toShow);
            var cols = _.values(metadata.showCols);
            _.each(cols, function(colName,i) {
                //cols[i].bVisible = true;
                if(!_.include(toShow, colName)) {
                    oTable.fnSetColumnVis(i,false);
                }
            });
            //var w = esta.width;
            //if(!w) {
                //w = oTable.width();
                //esta.width = w;
            //}
            //oTable.width(w+w*0.3);
            //oTable.fnDraw();
        },
        fnInitComplete: function() {
            $("table tr").first().click();
        },
        fnServerData: function ( sSource, aoData, fnCallback ) {
            var request = {};
            _.each(aoData, function(o) {
                request[o.name] = o.value;
            });
            esta.serverData({
                limit : request.iDisplayLength,
                skip : request.iDisplayStart
            }, function(data) {
                data = esta.transformer(data,esta.metadata);
                fnCallback({
                    iTotalRecords : esta.metadata.count,
                    iTotalDisplayRecords : esta.metadata.count,
                    sEcho : request.sEcho,
                    aaData : data
                });
            });
        }
    } );
    console.log(_.size(metadata.groups.labelColumns));
    new FixedColumns(oTable, {
        columns : _.size(metadata.groups.labelColumns)
    });
};

_.extend(iv.Table.prototype,iv.Module.prototype);


iv.Table.prototype.template = _.template("\
<table id='<%= id %>'>\
<thead><tr>\
<% _.each(metadata.showCols, function(col,i) { %>\
    <th>\
    <% if (metadata.isTimeColumn(col)) { %>\
        <%= metadata.nameProcessors.timeColNames(col) %>\
    <% } else { %>\
        <%= col %>\
    <% } %>\
    </th>\
<% }); %>\
</tr></thead>\
<tbody>\
<tbody>\
</table>\
");

iv.Table.prototype.render = _.identity;

iv.Table.prototype.showHide = function(iCol) {
    var oTable = $("#"+this.tableId).dataTable();
    var bVis = oTable.fnSettings().aoColumns[iCol].bVisible;
    oTable.fnSetColumnVis( iCol, bVis ? false : true );
};

