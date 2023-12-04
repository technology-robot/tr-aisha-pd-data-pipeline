import re


def filter(content):
    return True

def refine(content):
    title = content.title.strip()
    html = content.html

    # Remove unimportant text
    html = re.sub(r'Für den Versand von[\S\s]*?\/', '', html)
    html = re.sub(r'Downloads[\S\s]*?FAQ.', '', html)
    html = html.strip()

    related_product_match = re.search(r'Das könnte dir auch gefallen[\S\s]*Related products', html)
    if related_product_match is not None:
        html = html[:related_product_match.span()[0]] + html[related_product_match.span()[1]:]
    else:
        html = re.sub(r'Das könnte dir auch gefallen', '', html)

    html = html.strip()
    return (
        html,
        {
            "title": title,
            "url": content.url
        }
    )
