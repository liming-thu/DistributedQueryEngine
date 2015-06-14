$(document).ready(function(){
    $("button#summit").click(function(){
        var sql = $("textarea#sql").val();
        htmlobj=$.ajax({
            type: 'POST',
            url: "http://localhost:40425/QueryService.asmx/Sql2AlgTree",
            data: sql,
            dataType: 'xml',
            success: function(result) {
                $("p#algTree").text(result);
            },
            error: function(req, error) {
                console.log(req);
            }
        });
    });
});