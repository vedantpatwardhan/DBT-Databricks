apples = {"Gala", "Red Delicious", "Granny Smith", "Honeycrisp", "Fuji", "Braeburn", "Pink Lady", "McIntosh", "Cortland", "Empire"}

{% for i in apples %}
    {% if i != "Mcintosh" %}
        {{i}}
    {% else %}
        I hate {{i}}
    {% endif %}
{% endfor %}