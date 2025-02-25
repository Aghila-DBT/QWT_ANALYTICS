{% macro get_order_date_max() %}
 
    {% set get_max_order_date %}
 
        SELECT max(orderdate)
        from {{ref('f_orders')}}
      
 
    {% endset %}
 
    {% set results = run_query(get_max_order_date) %}
 
    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0][0] %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}
 
    {{ return(results_list) }}

{% endmacro %}