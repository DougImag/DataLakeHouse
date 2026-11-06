{#
  Tests génériques du projet (utilisables dans les fichiers .yml).
  Un test échoue s'il renvoie au moins une ligne.
#}

{# Aucune ligne ne partage la même combinaison de colonnes #}
{% test combinaison_unique(model, colonnes) %}
    select {{ colonnes | join(', ') }}, count(*) as nb
    from {{ model }}
    group by {{ colonnes | join(', ') }}
    having count(*) > 1
{% endtest %}

{# Valeurs >= 0 (les NULL sont ignorés) #}
{% test positif_ou_nul(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}

{# Valeurs conformes à une expression régulière (les NULL sont ignorés) #}
{% test format_regex(model, column_name, regex) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and not regexp_like({{ column_name }}, '{{ regex }}')
{% endtest %}

{# Valeurs comprises entre min et max inclus (les NULL sont ignorés) #}
{% test compris_entre(model, column_name, min, max) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < {{ min }}
       or {{ column_name }} > {{ max }}
{% endtest %}
