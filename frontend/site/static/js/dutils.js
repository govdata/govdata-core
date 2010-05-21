var DUtils = {};

(function($) {
    DUtils.map = function (value, istart, istop, ostart, ostop) {
        return ostart + (ostop - ostart) * ((value - istart) / (istop - istart));
    }
    DUtils.stopPropagation = function(ev) {
        if (!ev) var ev = window.event;
        ev.cancelBubble = true;
        if (ev.stopPropagation) ev.stopPropagation();
    }
    
    var keyConverter = function(ev) {
        var cmd = "";
        if(ev.altKey === true) { cmd = cmd+"alt+"; }
        if(ev.ctrlKey === true) { cmd = cmd+"ctrl+"; }
        if(ev.metaKey === true) { cmd = cmd+"meta+"; }
        if(ev.shiftKey === true) { cmd = cmd+"shift+"; }
        return cmd+keyLookUpTable[ev.which];
    }
    DUtils.keyHandlerByCase = function(cases) {
        return function(ev) {
            var propogate = true;
            var handler = cases[keyConverter(ev)];
            if(handler === undefined) {
                handler = cases['default'];
                if(handler === undefined) { return; }
            }
            propogate = handler(ev);
            if(propogate === false) {
                ev.preventDefault();
                DUtils.stopPropagation();
            }
        }
    }
})(jQuery);