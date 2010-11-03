module("Module Underscore Extensions");

test("difference calculation", function() {
    var a = {
        splat: "same",
        squish: {
            slam: "same",
            thing: {
                is: {
                    not: "same",
                    also: "same" }}},
        foo: "same",
        bar: {
            baz: "same" }};
    var b = {
        splat: "different",
        squish: {
            slam: "same",
            thing: {
                is: {
                    not: "different",
                    also: "same" }}},
        foo: "same",
        bar: {
            baz: "different" }};
    var diff = _.difference(a,b);
    var expected_diff = {
        splat: "different",
        squish: {
            thing: {
                is: {
                    not: "different"}}},
        bar: {
            baz: "different" }};
    same(diff, expected_diff);
});

test("areArray", function() {
    var a = [1,3,4];
    var b = [3,4,5];
    var c = {};
    var d = [1,3];
    equal(_.areArray(a,b),true);
    equal(_.areArray(a,c),false);
    equal(_.areArray(a,b,c),false);
    equal(_.areArray(a,b,d),true);
});

test("union", function() {
    var a = [1,3,4];
    var b = [3,5,2];
    var c = [3,4,6];
    same(_.union(a,b),[1,3,4,3,5,2]);
    same(_.union(a,b,c),[1,3,4,3,5,2,3,4,6]);
})

module("Module Common (C)");

module("Module GovLove");

module("Module Frontend");
